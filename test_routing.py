import sys
sys.path.insert(0, 'd:/Flight ETL integrated with agent')
from agent.sql_agent import generate_dynamic_sql, _wm, REVIEW_KEYWORDS

tests = [
    ('which airplane has the most passanger seat?',
     'total_passengers', 'passenger sort'),
    ('please provide me with 10 airline from the richest to poorest based on the revenue AND passanger total',
     'total_revenue', 'richest to poorest revenue'),
    ('please provide me with 10 airline from the richest to poorest based on the revenue AND passanger total in 10 years',
     'total_revenue', 'richest to poorest 10 years'),
    ('which airline has the most bad reviews?',
     'fact_customer_feedback', 'bad reviews routing'),
    ('which airline has the highest average departure delay?',
     'departure_delay', 'delay routing'),
    ('show monthly revenue trend',
     'dd.month', 'monthly trend routing'),
    ('top 3 busiest destination cities',
     'destination_city', 'busiest destination cities'),
    ('what are the top complaint categories?',
     'complaint_category', 'top complaint categories'),
    ('show quarterly revenue trend',
     'dd.quarter', 'quarterly trend routing'),
    ('airlines with the worst lateness',
     'avg_departure_delay', 'lateness delay routing'),
    ('top grossing airlines',
     'total_revenue', 'top grossing revenue'),
    ('please provide me with the most good reviewed airline',
     'fact_customer_feedback', 'most good reviewed routing'),
]

all_pass = True
for prompt, expected, label in tests:
    sql = generate_dynamic_sql(prompt)
    ok = expected in sql
    status = 'PASS' if ok else 'FAIL'
    if not ok:
        all_pass = False
    print(status + ' ' + label)
    if not ok:
        print('  expected: ' + expected)
        print('  got sql : ' + sql[:120])

false_pos = _wm(REVIEW_KEYWORDS, 'richest to poorest')
fp_ok = not false_pos
print(('PASS' if fp_ok else 'FAIL') + ' poorest does not trigger REVIEW domain')
if not fp_ok:
    all_pass = False

print()
print('ALL PASSED' if all_pass else 'SOME FAILED')

