i = int(input('Какой по счету день недели?'))

if i == 1:
    import MondayOKK
    import ZakrZavtraMon
elif i == 5:
    import EveryDayOKK
    import ZakrZavtraFRIDAY
else:
    import ZakrZavtraMon
    import EveryDayOKK
