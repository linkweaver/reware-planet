# -*- coding: utf-8 -*-
# Part of Odoo. See LICENSE file for full copyright and licensing details.

import base64
import codecs
from collections import defaultdict
import csv
import datetime
import io
import logging

from odoo import _, fields, models
from odoo.exceptions import UserError, RedirectWarning
from odoo.tools import float_repr, float_is_zero, get_lang

_logger = logging.getLogger(__name__)


class FecImportWizard(models.TransientModel):
    _inherit = "account.fec.import.wizard"
    
    # ------------------------------------
    # Generators
    # -----------------------------------
   
    def _generator_fec_res_partner(self, rows, cache):
        """ Import the partners from FEC data files """

        partners_set = set()
        for record in rows:
            partner_ref = record.get("CompAuxNum", "")
            partner_name = record.get("CompAuxLib", "")
            account_code = record.get("CompteNum", "")

            # Check for an existing partner with both the same name and ref
            if partner_name and (partner_name, partner_ref) not in partners_set:
                partners_set.add((partner_name, partner_ref))

                # Check if the partner is already existing
                existing = cache["res.partner"].get(partner_name, None)
                if not existing or (partner_ref and partner_ref != existing.ref):
                    data = {
                        "company_id": self.company_id.id,
                        "name": partner_name,
                        "ref": partner_ref,
                    }

                    # Setup account properties
                    account = account_code and cache["account.account"].get(account_code.rstrip('0'), None)
                    if account:
                        if account.account_type == 'asset_receivable':
                            data["property_account_receivable_id"] = account.id
                        elif account.account_type == 'liability_payable':
                            data["property_account_payable_id"] = account.id

                    yield data

    def _generator_fec_account_move(self, rows, cache):
        """ Import the moves from the FEC files.
            The first loop collects informations, then in a second loop, move_line level information is assembled and the data can be yielded.

            If partner information is found on a line, it has to be brought from move_line level to move level.

            The credit/debit may be specified as Montant/Sens.
            Sens must be in ['C', 'D'] which mean Credit/Debit).
        """

        moves_dict = {}

        # Keeps track of moves grouped by journal_id and move_date, it helps with imbalances
        imbalances = defaultdict(lambda: defaultdict(list))

        # Keeps the move's balance after summing each line's debit and credit
        balance_dict = {}

        for idx, record in enumerate(rows):

            # Move data -----------------------------------------

            # The move_name sometimes may be not provided, use the piece_ref instead
            piece_ref = record.get("PieceRef", "")
            ecriture_num = record.get("EcritureNum", "")
            move_name = ecriture_num or piece_ref
            if not move_name:
                raise UserError(_("Line %s has an invalid move name", idx))

            # The move_date sometimes is not provided, use the piece_date instead
            piece_date = record.get("PieceDate", "")
            piece_date = piece_date and datetime.datetime.strptime(piece_date, "%Y%m%d")
            move_date = record.get("EcritureDate", "")
            move_date = (move_date and datetime.datetime.strptime(move_date, "%Y%m%d")) or piece_date
            partner_ref = record.get("CompAuxNum", "")
            partner_name = record.get("CompAuxLib", "")
            journal_code = record.get("JournalCode", "")

            # Move line data ------------------------------------
            move_line_name = record.get("EcritureLib", "")
            account_code = record.get("CompteNum", "")
            currency_name = record.get("Idevise", "")
            amount_currency = self._normalize_float_value(record, "Montantdevise")
            matching = record.get("EcritureLet", "")

            # Move import --------------------------------------

            # Journal
            journal = cache["account.journal"].get(journal_code, None)
            if not journal:

                # Look for a shortened code
                journal_code = cache.get("mapping_journal_code", {}).get(journal_code, None)
                if journal_code:
                    journal = cache["account.journal"].get(journal_code, None)

                if not journal:
                    raise UserError(_("Line %s has an invalid journal code", idx))

            # Use the journal and the move_name as key for the move in the moves_dict
            move_key = "%s/%s" % (journal.code, move_name)

            # Many move_lines may belong to the same move, the move info gets saved in the moves_dict
            data = moves_dict.get(move_key, {
                "company_id": self.company_id.id,
                "name": move_name,
                "date": move_date,
                "ref": piece_ref,
                "journal_id": journal.id,
                "line_ids": [],
            })
            balance_data = balance_dict.get(move_key, {"balance": 0.0, "matching": False})

            # Move line import ----------------------------------

            # Account
            account = cache["account.account"].get(account_code.rstrip('0'), None)
            if not account:
                raise UserError(_("Line %s has an invalid account %r", idx, account_code))

            # Build the basic data
            line_data = {
                "company_id": self.company_id.id,
                "name": move_line_name,
                "ref": piece_ref,
                "account_id": account.id,
                "fec_matching_number": matching or False,
            }

            # Save the matching number for eventual balance issues
            balance_data["matching"] = balance_data["matching"] or matching or False

            # Partner. As we are creating Journal Entries and not invoices/vendor bills,
            # the partner information will stay just on the line.
            # It may be updated in the post-processing after all the imports are done.
            if partner_ref:
                partner = cache["res.partner.ref"].get(partner_ref, None)
                line_data["partner_id"] = partner.id if partner else False

            # Currency
            if currency_name in cache["res.currency"]:
                currency = cache["res.currency"][currency_name]
                line_data.update({
                    "currency_id": currency.id,
                    "amount_currency": amount_currency,
                })
            else:
                currency = self.company_id.currency_id

            # Round the values, save the total balance to detect issues
            credit, debit, balance = self._get_credit_debit_balance(record, currency)
            line_data["credit"] = credit
            line_data["debit"] = debit
            balance_data["balance"] = currency.round(balance_data["balance"] + balance)

            # Montantdevise can be positive while the line is credited:
            # => amount_currency and balance (debit - credit) should always have the same sign
            if currency_name in cache["res.currency"] and line_data['amount_currency'] * balance < 0:
                line_data["amount_currency"] *= -1

            # Append the move_line data to the move
            data["line_ids"].append(fields.Command.create(line_data))

            # Update the data in the moves_dict
            moves_dict[move_key] = data
            imbalances[journal.id][move_date].append(line_data)
            balance_dict[move_key] = balance_data

        # Check for imbalanced journals, fix rounding issues
        imbalanced_journals = self._check_rounding_issues(moves_dict, balance_dict)

        # If there are still imbalanced, journals, try to re-group the lines by journal/date,
        # to see if now they balance altogether
        if imbalanced_journals:
            self._check_imbalanced_journals(cache, moves_dict, balance_dict, imbalanced_journals, imbalances)

        yield from moves_dict.values()

    

    # ------------------------------------
    # Utility functions
    # ------------------------------------

    

    # -----------------------------------
    # Main methods
    # -----------------------------------

    def _import_files(self, models=None):
        """ Start the import by gathering generators and templates and applying them to attached files. """

        # Basic checks to start
        if not self.company_id.account_fiscal_country_id or not self.company_id.chart_template_id:
            action = self.env.ref('account.action_account_config')
            raise RedirectWarning(_('You should install a Fiscal Localization first.'), action.id, _('Accounting Settings'))

        # Models list can be injected for testing purposes
        if not models:
            models = ["account.account", "account.journal", "account.move"]

        # In Odoo, move names follow sequences based on the year, so the checks complain
        # if the year present in the move's name doesn't match with the move's date.
        # This is unimportant here since we are importing existing moves from external data.
        # The workaround is to set the sequence.mixin.constraint_start_date parameter
        # to the date of the oldest move (defaulting to today if there is no move at all).
        if "account.move" in models:
            domain = [("company_id", "=", self.company_id.id)]
            start_date = self.env["account.move"].search(domain, limit=1, order="date asc").date or fields.Date.today()
            start_date_str = start_date.strftime("%Y-%m-%d")
            self.env["ir.config_parameter"].sudo().set_param("sequence.mixin.constraint_start_date", start_date_str)

        # Build a cache with all the cache needed by the generators, so that the query is done just one time
        cache = self._build_import_cache()

        all_records = {}
        all_templates = self._gather_templates()
        rows = self._get_rows(self.attachment_id, self.attachment_name)

        # For each file provided, cycle over each model
        for model in models:

            _logger.info("%s FEC import started", model)

            # Retrieve the templates
            model_templates = all_templates.get(model, {})

            # Generate the records for the model
            records = []
            generator_name = "_generator_fec_%s" % model.replace(".", "_")
            generator = getattr(self, generator_name)

            # Loop over generated records and apply a template if a matching one is found
            for idx, record in enumerate(generator(rows, cache)):
                self._apply_template(model_templates, model, record)
                records.append({"values": record})

                # Notify the user every 100 records
                if idx and idx % 100 == 0:
                    _logger.info("%5d records gathered", idx)

            # Import records, then flush and update the cache with the inserted records
            if records:
                all_records[model] = self.env[model]._load_records(records)
                self._update_import_cache(cache, model, all_records[model])

        # If there are moves, post them
        moves = all_records.get("account.move", [])
        if moves:
            _logger.info("Posting moves...")
            moves.action_post()

            _logger.info("Reconciling move_lines...")
            self._reconcile_imported_move_lines(moves)

            journals = all_records.get("account.journal", [])
            if journals:
                journals_dict = {journal.id: journal for journal in cache["account.journal"].values()}
                for journal_id, journal_type in self._get_journal_type(journals, ratio=0.7, min_moves=3):
                    journal = journals_dict[journal_id]
                    journal.type = journal_type

                    # The bank journal needs a default liquidity account and outstanding payments accounts to be set
                    if journal_type == 'bank':
                        self._setup_bank_journal(journal)

                self._post_process(journals, moves)

        return {
            "type": "ir.actions.client",
            "tag": "reload",
        }
