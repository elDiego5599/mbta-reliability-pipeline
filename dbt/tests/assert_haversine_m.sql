select * from (values
    ('one degree on a meridian', {{ haversine_m('42.0', '-71.0', '43.0', '-71.0') }}, 111195.0),
    ('same point', {{ haversine_m('42.35', '-71.06', '42.35', '-71.06') }}, 0.0)
) as t(case_name, got, want)
where abs(got - want) >= 1
