from decimal import Decimal
from uuid import uuid4
from demo.app.exports import quantity_number,render_summary

def test_quantity_precision():
 assert quantity_number(Decimal('0.115'))=='0,115'
 assert quantity_number(Decimal('1200'))=='1\u00a0200'
 assert quantity_number(Decimal('0.000001'))=='0,000001'

def test_comtrade_fraction_in_summary():
 rows=[dict(strana='BA',type='fact',period='2026-08-01',kol=.115,edizm='ШТУКА',edizm_iso='796',quality_flags='Количество оценено источником Comtrade')]
 text=render_summary(rows,{('BA','fact'):[Decimal(3070),Decimal(2570)]},{'stoim','netto','kol'},{'tnved4':['8703']},{},uuid4())
 assert '0,115' in text
 assert 'количество включает оценку Comtrade' in text
 assert '| ШТУКА | 0 |' not in text


def test_known_quantity_is_not_full_country_total():
 rows=[dict(strana='IN',type='fact',period='2026-07',kol=23,edizm='ШТУКА',edizm_iso='796'),
       dict(strana='IN',type='fact',period='2026-07',kol=None,edizm='?',edizm_iso=None)]
 text=render_summary(rows,{('IN','fact'):[Decimal(100),Decimal(0)]},{'stoim','netto','kol'},{'tnved4':['8703']},{},uuid4())
 assert '23 (только известная часть; не полный итог)' in text
 assert '**Количество по факту:' not in text
 assert 'нет полных данных' in text
