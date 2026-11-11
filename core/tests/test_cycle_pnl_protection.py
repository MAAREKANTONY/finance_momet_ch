from datetime import date, timedelta
from decimal import Decimal
import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.forms import BacktestForm
from core.models import Alert, Backtest, DailyBar, DailyMetric, Scenario, Symbol
from core.services.backtesting.engine import run_backtest, run_backtest_kpi_only


class CyclePnlProtectionTests(TestCase):
    def setUp(self):
        self.start = date(2024, 1, 1)
        self.scenario = Scenario.objects.create(name='Cycle PnL', a=1, b=1, c=1, d=1)
        self.a = Symbol.objects.create(ticker='AAA', exchange='NYSE', active=True)
        self.b = Symbol.objects.create(ticker='BBB', exchange='NYSE', active=True)
        self.scenario.symbols.set([self.a, self.b])

    def day(self, offset):
        return self.start + timedelta(days=offset)

    def prices(self, symbol, values):
        for offset, price in enumerate(values):
            if price is None:
                continue
            value = Decimal(str(price))
            DailyBar.objects.create(symbol=symbol, date=self.day(offset), open=value, high=value, low=value, close=value)
            DailyMetric.objects.create(symbol=symbol, scenario=self.scenario, date=self.day(offset), P=value, ratio_P=1)

    def signal(self, symbol, offset, codes='A1'):
        Alert.objects.update_or_create(symbol=symbol, scenario=self.scenario, date=self.day(offset), defaults={'alerts': codes})

    def backtest(self, days, **overrides):
        options = dict(name='Cycle PnL', scenario=self.scenario, start_date=self.start, end_date=self.day(days-1),
                       capital_total=10000, capital_per_ticker=10000, capital_mode='REINVEST', include_all_tickers=True,
                       universe_snapshot=['AAA'], signal_lines=[{'buy': ['A1'], 'sell': ['B1'], 'trading_model': 'LEGACY_DAILY'}],
                       close_positions_at_end=False,
                       settings={'portfolio_protection': {'enabled': True, 'mode': 'cycle_pnl', 'threshold_pct': '1', 'initial_loss_amount': '100'}})
        options.update(overrides)
        return Backtest.objects.create(**options)

    def test_profit_peak_example_distinguishes_pnl_from_portfolio_value(self):
        self.prices(self.a, ['100', '120', '119.8', '119.7'])
        self.signal(self.a, 0)
        for mode in ('FIXED', 'REINVEST'):
            for large in (False, True):
                with self.subTest(mode=mode, large=large):
                    bt = self.backtest(4, capital_mode=mode)
                    result = run_backtest(bt, large_result_mode=large).results
                    rows = result['portfolio']['daily']
                    self.assertFalse(rows[2]['protection_liquidation'])  # +1980 == 2000 * .99
                    self.assertTrue(rows[3]['protection_liquidation'])   # +1970 < +1980
                    cycle = result['portfolio']['protection']['cycles'][0]
                    self.assertEqual(Decimal(cycle['trigger_pnl']), 1970)
                    self.assertEqual(Decimal(cycle['trigger_peak_pnl']), 2000)
                    self.assertEqual(Decimal(cycle['trigger_bound']), 1980)
                    self.assertEqual(cycle['trigger_date'], str(self.day(3)))
                    self.assertEqual(Decimal(rows[2]['protection_decision_pnl']), 1980)
                    self.assertEqual(Decimal(result['portfolio']['kpi']['TOTAL_PNL_AMOUNT']), 1970)
                    bt.settings = {'portfolio_protection': {'enabled': True, 'mode': 'portfolio_value', 'threshold_pct': '1'}}
                    legacy = run_backtest(bt, large_result_mode=large).results
                    self.assertFalse(any(row['protection_liquidation'] for row in legacy['portfolio']['daily']))
                    self.assertEqual(Decimal(legacy['portfolio']['daily'][-1]['protection_drawdown']), Decimal('.0025'))

    def test_initial_loss_equality_and_strict_breach_without_positive_peak(self):
        self.prices(self.a, ['100', '99', '98.99'])
        self.signal(self.a, 0)
        for capital in (0, 10000):
            for mode in ('FIXED', 'REINVEST'):
                with self.subTest(capital=capital, mode=mode):
                    bt = self.backtest(3, capital_total=capital, capital_mode=mode)
                    # Before the first positive peak, L applies independently of X.
                    bt.settings['portfolio_protection']['threshold_pct'] = '25'
                    result = run_backtest(bt).results
                    rows = result['portfolio']['daily']
                    self.assertFalse(rows[1]['protection_liquidation'])
                    self.assertTrue(rows[2]['protection_liquidation'])
                    cycle = result['portfolio']['protection']['cycles'][0]
                    self.assertEqual(Decimal(cycle['trigger_pnl']), -101)
                    self.assertEqual(Decimal(cycle['trigger_peak_pnl']), 0)
                    self.assertEqual(Decimal(cycle['trigger_bound']), -100)
                    self.assertEqual(cycle['trigger_rule'], 'initial_loss')

    def test_any_positive_peak_switches_from_initial_loss_to_profit_retracement(self):
        self.prices(self.a, ['100', '100.0001', '100'])
        self.signal(self.a, 0)
        result = run_backtest(self.backtest(3)).results
        self.assertTrue(result['portfolio']['daily'][-1]['protection_liquidation'])
        cycle = result['portfolio']['protection']['cycles'][0]
        self.assertEqual(Decimal(cycle['trigger_peak_pnl']), Decimal('.01'))
        self.assertEqual(Decimal(cycle['trigger_bound']), Decimal('.0099'))
        self.assertEqual(cycle['trigger_rule'], 'profit_retracement')

    def test_multiple_cycles_preserve_global_and_realized_pnl_and_block_same_day_rebuy(self):
        self.prices(self.a, ['100', '120', '119.7', '100', '102', '101.98', '101.97', '100'])
        for offset in (0, 2, 3, 5, 6, 7):
            self.signal(self.a, offset)
        for capital in (0, 10000):
            for mode, second_gain in (('FIXED', Decimal('197')), ('REINVEST', Decimal('234.43'))):
                with self.subTest(capital=capital, mode=mode):
                    result = run_backtest(self.backtest(8, capital_total=capital, capital_mode=mode)).results
                    events = result['tickers']['AAA']['lines'][0]['events']
                    self.assertEqual([(e['date'], e['action']) for e in events],
                                     [(str(self.day(i)), action) for i, action in [(0,'BUY'),(2,'SELL'),(3,'BUY'),(6,'SELL'),(7,'BUY')]])
                    cycles = result['portfolio']['protection']['cycles']
                    self.assertEqual(len(cycles), 3)
                    self.assertEqual([Decimal(c['pnl']) for c in cycles], [Decimal('1970'), second_gain, Decimal('0')])
                    self.assertEqual(Decimal(result['portfolio']['kpi']['TOTAL_PNL_AMOUNT']), Decimal('1970') + second_gain)
                    realized = sum(Decimal(e['action_PNL_AMOUNT']) for e in events if e['action']=='SELL')
                    self.assertEqual(realized, Decimal('1970') + second_gain)
                    self.assertEqual(Decimal(result['portfolio']['daily'][7]['protection_cycle_pnl']), 0)
                    self.assertEqual(Decimal(result['portfolio']['daily'][7]['protection_pnl_peak']), 0)

    def test_ordinary_sale_transfers_latent_to_realized_without_resetting_cycle(self):
        self.prices(self.a, ['100', '120', '120', '120', '119.7'])
        self.signal(self.a, 0)
        self.signal(self.a, 2, 'B1')
        self.signal(self.a, 3)
        for mode in ('FIXED', 'REINVEST'):
            with self.subTest(mode=mode):
                result = run_backtest(self.backtest(5, capital_mode=mode)).results
                rows = result['portfolio']['daily']
                for i in (1, 2, 3):
                    self.assertEqual(Decimal(rows[i]['protection_cycle_pnl']), 2000)
                    self.assertEqual(Decimal(rows[i]['protection_pnl_peak']), 2000)
                    self.assertEqual(Decimal(rows[i]['protection_pnl_bound']), 1980)
                    self.assertEqual(rows[i]['protection_cycle'], 1)
                self.assertTrue(rows[4]['protection_liquidation'])

    def test_unlimited_funding_creates_neither_profit_nor_a_new_peak(self):
        self.prices(self.a, ['100', '120', '120', '119.8', '119.7'])
        self.prices(self.b, ['100'] * 5)
        self.signal(self.a, 0)
        self.signal(self.b, 2)
        for mode in ('FIXED', 'REINVEST'):
            with self.subTest(mode=mode):
                result = run_backtest(self.backtest(5, capital_total=0, capital_mode=mode, universe_snapshot=['AAA','BBB'])).results
                rows = result['portfolio']['daily']
                self.assertEqual(Decimal(rows[2]['equity']), 22000)
                self.assertEqual(Decimal(rows[2]['protection_cycle_pnl']), 2000)
                self.assertEqual(Decimal(rows[2]['protection_pnl_peak']), 2000)
                self.assertFalse(rows[3]['protection_liquidation'])
                self.assertTrue(rows[4]['protection_liquidation'])
                cycle = result['portfolio']['protection']['cycles'][0]
                self.assertEqual(Decimal(cycle['external_funding']), 10000)
                self.assertEqual(Decimal(cycle['pnl']), 1970)
                self.assertEqual(Decimal(rows[4]['pnl_global']), 1970)

    def test_unlimited_funding_during_initial_loss_does_not_mask_the_loss(self):
        self.prices(self.a, ['100', '99', '98.99'])
        self.prices(self.b, ['100'] * 3)
        self.signal(self.a, 0)
        self.signal(self.b, 1)
        result = run_backtest(self.backtest(3, capital_total=0, universe_snapshot=['AAA','BBB'])).results
        rows = result['portfolio']['daily']
        self.assertEqual(Decimal(rows[1]['protection_cycle_pnl']), -100)
        self.assertEqual(Decimal(rows[1]['protection_pnl_peak']), 0)
        self.assertFalse(rows[1]['protection_liquidation'])
        self.assertTrue(rows[2]['protection_liquidation'])

    def test_missing_quotes_delay_sales_and_keep_exact_trigger_snapshot_and_block_buys(self):
        self.prices(self.a, ['100', '120', None, None, '119.7', '100'])
        self.prices(self.b, ['100', '100', '99.4', '99.3', '99.3', '99.3'])
        for symbol in (self.a, self.b):
            for offset in range(6):
                self.signal(symbol, offset)
        for mode in ('FIXED','REINVEST'):
            with self.subTest(mode=mode):
                result = run_backtest(self.backtest(6, capital_total=20000, capital_mode=mode, universe_snapshot=['AAA','BBB'])).results
                rows = result['portfolio']['daily']
                cycle = result['portfolio']['protection']['cycles'][0]
                self.assertEqual(cycle['trigger_date'], str(self.day(2)))
                self.assertEqual(Decimal(cycle['trigger_pnl']), 1940)
                self.assertEqual(Decimal(cycle['trigger_bound']), 1980)
                self.assertEqual(cycle['end_date'], str(self.day(4)))
                self.assertTrue(rows[3]['protection_pending'])
                self.assertEqual(Decimal(rows[4]['protection_cycle_pnl']), 1910)
                for ticker, sale_day in (('AAA',4),('BBB',2)):
                    events = result['tickers'][ticker]['lines'][0]['events']
                    self.assertEqual([(e['date'],e['action']) for e in events],
                                     [(str(self.day(0)),'BUY'),(str(self.day(sale_day)),'SELL'),(str(self.day(5)),'BUY')])

    def test_legacy_value_mode_and_disabled_results_are_unchanged(self):
        self.prices(self.a, ['100','120','100','90'])
        self.signal(self.a, 0)
        bt = self.backtest(4, settings={'portfolio_protection': {'enabled': True, 'threshold_pct':'1'}})
        legacy = run_backtest(bt).results
        bt.settings['portfolio_protection']['mode'] = 'portfolio_value'
        self.assertEqual(run_backtest(bt).results, legacy)
        bt.settings = {}
        disabled = run_backtest(bt).results
        bt.settings = {'portfolio_protection': {'enabled': False, 'mode':'cycle_pnl','threshold_pct':'bad','initial_loss_amount':'bad'}}
        self.assertEqual(run_backtest(bt).results, disabled)

    def test_kpi_only_keeps_numeric_best_bmd_with_pnl_protection(self):
        self.prices(self.a, ['100','120','119.8','119.7'])
        self.signal(self.a,0)
        bt=self.backtest(4,signal_lines=[{'buy':['A2'],'sell':['B2'],'trading_model':'LEGACY_DAILY'},
                                       {'buy':['A1'],'sell':['B1'],'trading_model':'LEGACY_DAILY'}])
        out=run_backtest_kpi_only(bt)['AAA']
        full=run_backtest(bt).results['tickers']['AAA']
        self.assertEqual(out['best_bmd'],full['lines'][1]['final']['BMD'])
        self.assertIsNone(out['lines'][0]['final']['BMD'])
        self.assertEqual(Decimal(out['best_bmd']),Decimal('.197')/3)
        for i,line in enumerate(out['lines']):
            self.assertEqual(line['final'],full['lines'][i]['final'])

    def test_engine_rejects_invalid_pnl_configuration(self):
        for loss in (None, 'bad', 'NaN', 'Infinity', '0', '-1'):
            with self.subTest(loss=loss):
                bt=self.backtest(1)
                bt.settings['portfolio_protection']['initial_loss_amount']=loss
                with self.assertRaises(ValueError):
                    run_backtest(bt)
        bt=self.backtest(1)
        bt.settings['portfolio_protection']['mode']='unknown'
        with self.assertRaises(ValueError):
            run_backtest_kpi_only(bt)

    def test_zero_quote_keeps_liquidation_pending_until_executable_price(self):
        self.prices(self.a, ['100', '0', '98', '100'])
        for offset in range(4):
            self.signal(self.a, offset)
        result = run_backtest(self.backtest(4)).results
        rows = result['portfolio']['daily']
        self.assertTrue(rows[1]['protection_pending'])
        self.assertFalse(rows[1]['protection_liquidation'])
        self.assertTrue(rows[2]['protection_liquidation'])
        events = result['tickers']['AAA']['lines'][0]['events']
        self.assertEqual([(e['date'], e['action']) for e in events],
                         [(str(self.day(0)), 'BUY'), (str(self.day(2)), 'SELL'), (str(self.day(3)), 'BUY')])

    def test_exhausted_fixed_reserve_does_not_create_a_funded_cycle(self):
        self.prices(self.a, ['100', '1', '100', '1', '100', '100'])
        self.prices(self.b, ['100'] * 6)
        for offset in range(6):
            self.signal(self.a, offset)
        self.signal(self.b, 5)
        for capital in (0, 10000):
            with self.subTest(capital=capital):
                result = run_backtest(self.backtest(6, capital_total=capital, capital_mode='FIXED', universe_snapshot=['AAA','BBB'])).results
                rows = result['portfolio']['daily']
                self.assertEqual(Decimal(rows[4]['equity']), -9800)
                self.assertEqual(Decimal(rows[4]['positions_value']), 0)
                self.assertIsNone(rows[4]['protection_cycle'])
                self.assertIsNone(rows[4]['protection_cycle_pnl'])
                if capital == 0:
                    self.assertEqual(Decimal(rows[5]['equity']), 200)
                    self.assertEqual(Decimal(rows[5]['protection_cycle_pnl']), 0)
                else:
                    self.assertIsNone(rows[5]['protection_cycle'])

    def form_data(self, **overrides):
        data=dict(name='PnL form',scenario=self.scenario.pk,start_date=str(self.start),end_date=str(self.day(3)),
                  capital_total='10000',capital_per_ticker='10000',capital_mode='FIXED',ratio_threshold='0',warmup_days='0',
                  signal_lines=json.dumps([{'buy':['A1'],'sell':['B1'],'trading_model':'LEGACY_DAILY'}]),
                  portfolio_protection_mode='cycle_pnl',portfolio_drawdown_threshold='1',portfolio_initial_loss_amount='100')
        data.update(overrides)
        return data

    def test_form_modes_validation_legacy_default_and_snapshot_restore(self):
        choices=dict(BacktestForm().fields['portfolio_protection_mode'].choices)
        self.assertEqual(set(choices),{'disabled','portfolio_value','cycle_pnl'})
        for field,bad_values in [('portfolio_initial_loss_amount',('','0','-1','NaN','Infinity')),
                                 ('portfolio_drawdown_threshold',('','0','100','NaN','-1'))]:
            for value in bad_values:
                with self.subTest(field=field,value=value):
                    form=BacktestForm(data=self.form_data(**{field:value}))
                    self.assertFalse(form.is_valid())
                    self.assertIn(field,form.errors)
        form=BacktestForm(data=self.form_data())
        self.assertTrue(form.is_valid(),form.errors)
        bt=form.save()
        self.assertEqual(bt.settings['portfolio_protection'],{'enabled':True,'mode':'cycle_pnl','threshold_pct':'1','initial_loss_amount':'100'})
        self.assertEqual(BacktestForm(instance=bt).fields['portfolio_protection_mode'].initial,'cycle_pnl')
        from core.services.run_configuration_snapshots import capture_backtest_configuration,restore_backtest_snapshot
        self.assertEqual(restore_backtest_snapshot(capture_backtest_configuration(bt)).settings,bt.settings)
        bt.settings={'portfolio_protection':{'enabled':True,'threshold_pct':'1'}}
        self.assertEqual(BacktestForm(instance=bt).fields['portfolio_protection_mode'].initial,'portfolio_value')

    def test_changing_only_pnl_loss_limit_invalidates_results(self):
        self.client.force_login(get_user_model().objects.create_user(username='pnl-editor'))
        bt=self.backtest(4,status='DONE',results={'stale':True})
        response=self.client.post(reverse('backtest_update',args=[bt.pk]),self.form_data(portfolio_initial_loss_amount='101'))
        self.assertEqual(response.status_code,302)
        bt.refresh_from_db()
        self.assertEqual(bt.results,{})
        self.assertEqual(bt.settings['portfolio_protection']['initial_loss_amount'],'101')

    def test_legacy_form_round_trip_keeps_implicit_value_mode_and_other_settings(self):
        bt = self.backtest(4, settings={'portfolio_protection': {'enabled': True, 'threshold_pct': '1'}, 'unrelated': 'preserve'})
        form = BacktestForm(instance=bt, data=self.form_data(portfolio_protection_mode='portfolio_value', portfolio_initial_loss_amount=''))
        self.assertTrue(form.is_valid(), form.errors)
        self.assertEqual(form.save().settings, {'portfolio_protection': {'enabled': True, 'threshold_pct': '1'}, 'unrelated': 'preserve'})

    def test_old_results_keep_their_mode_and_do_not_acquire_pnl_series(self):
        self.prices(self.a, ['100', '120', '119.7'])
        self.signal(self.a, 0)
        bt = self.backtest(3, settings={'portfolio_protection': {'enabled': True, 'threshold_pct': '1'}})
        old_result = run_backtest(bt).results
        old_result['portfolio']['protection'].pop('mode')
        bt.results = old_result
        bt.settings = self.backtest(1).settings
        bt.save()
        self.client.force_login(get_user_model().objects.create_user(username='legacy-pnl-reader'))
        with patch('core.views.build_diagnostic_chart_payload', return_value=None):
            response = self.client.get(reverse('backtest_results', args=[bt.pk]))
        self.assertContains(response, '<b>Recul de la valeur du portefeuille</b>', html=True)
        self.assertNotContains(response, 'id="cyclePnlProtectionChart"')
        self.assertNotContains(response, 'id="cycle-pnl-protection"')
        bt.refresh_from_db()
        self.assertEqual(bt.results, old_result)

    def test_results_show_saved_mode_amounts_and_exact_decision_separately_from_sales(self):
        self.prices(self.a,['100','120','119.8','119.7'])
        self.signal(self.a,0)
        bt=self.backtest(4)
        bt.results=run_backtest(bt).results
        bt.results['meta']['effective_currency'] = 'CNY'
        bt.settings={'portfolio_protection':{'enabled':True,'mode':'portfolio_value','threshold_pct':'99'}}
        bt.status='DONE'
        bt.save()
        self.client.force_login(get_user_model().objects.create_user(username='pnl-reader'))
        with patch('core.views.build_diagnostic_chart_payload',return_value=None):
            response=self.client.get(reverse('backtest_results',args=[bt.pk]))
        self.assertEqual(response.status_code,200)
        self.assertContains(response,'Mode enregistré :')
        self.assertContains(response,'Repli du PnL par cycle')
        self.assertContains(response,'PnL du cycle et borne de liquidation')
        self.assertContains(response,'PnL au déclenchement')
        self.assertContains(response,'Sommet du PnL')
        self.assertContains(response,'Borne de liquidation')
        self.assertContains(response,'id="cycle-pnl-protection"')
        self.assertContains(response,'exprimés en CNY')
        self.assertContains(response,'Montant (CNY)')
        self.assertEqual(response.context['results']['portfolio']['protection']['mode'],'cycle_pnl')
