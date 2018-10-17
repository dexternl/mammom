# script: queries_mammom.py
# analyseer database met verschillende queries
# creator: Erik-Jan de Kruijf
# datum: 14-10-2018

__version__ = 0.1

import sqlite3, csv, platform
import filescript_mammom


def geimporteerde_bestanden():
    #Geef een overzicht van de geimporteerde bestanden
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    rows = cursor.execute('''
    select
    *
    from
    csvbestanden
    '''
    )
    
    for row in rows:
        print('\t{}'.format(row[0]))
    connection.commit()
    connection.close()
    print()
    filescript_mammom.menu()

def lijst_rekeningnummers():
    #Geef een overzicht van de geimporteerde bestanden
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    rows = cursor.execute('''
    select
    *
    from
    rekeningnummers
    '''
    )
    print()
    print('\tRekeningnummer\t\tTenaamstelling')
    print()
    for row in rows:
        print('\t{}\t{}'.format(row[0],row[1]))
    connection.commit()
    connection.close()
    filescript_mammom.menu()

def lijst_rekeningnummers_zonder_menu():
    #Geef een overzicht van de geimporteerde bestanden
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    rows = cursor.execute('''
    select
    rowid, rekeningnummer, tenaamstelling
    from
    rekeningnummers
    '''
    )
    
    for row in rows:
        print('\t{}\t{}\t{}'.format(row[0],row[1], row[2]))
    connection.commit()
    connection.close()

def overeenkomstige_transacties():
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    

    keuze1 = input('Kies nummer: ')
    keuze2 = input('Kies nummer: ')

    query = '''
    SELECT

    rekeningnummer

    from
    rekeningnummers


    where
    rowid like "%" || ? || "%" '''

    rekening1 = cursor.execute(query, keuze1).fetchone()
    rekening1 = str(rekening1)
    rekening1 = rekening1[2:-3]

    rekening2 = cursor.execute(query, keuze2).fetchone()   
    rekening2 = str(rekening2)
    rekening2 = rekening2[2:-3]
    try:
        data = cursor.execute('''SELECT distinct*  FROM '''+rekening1+'''
    INNER JOIN '''+rekening2+''' ON '''+rekening1+'''.Tegenrekening = '''+rekening2+'''.Tegenrekening
    where '''+rekening1+'''.Tegenrekening != "N/A"

    UNION ALL

    SELECT distinct *  FROM '''+rekening2+'''
    INNER JOIN '''+rekening1+''' ON '''+rekening2+'''.Tegenrekening = '''+rekening1+'''.Tegenrekening
    where '''+rekening2+'''.Tegenrekening != "N/A";''')
    
        with open('overeenkomsten_'+rekening1+'_'+rekening2+'''.csv''', 'w+') as f:
            writer = csv.writer(f)
            writer.writerow(['Unieke_transactiecode', 'Rekeningnummer', 'Rekenhouder_of_tenaamgestelde', 'Pasvolgnummer', 'Transactiedatum', 'Tijdstip_transactie', 'Tijdzone_transactie', 'Boekdatum', 'Valutadatum', 'Type_transactie', 'BIC_code_transactie', 'Omschrijving', 'Transactiebedrag_Debet', 'Transactiebedrag_Credit', 'Valuta_transactie', 'Tegenrekening', 'Tenaamstelling_tegenrekening'])
            writer.writerows(data)
        filescript_mammom.menu()

    except sqlite3.OperationalError:
        print()
        print('fout in de zoekvraag probeer opnieuw')

        filescript_mammom.menu()

def keuze():
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    query = '''
    SELECT

    rekeningnummer

    from
    rekeningnummers


    where
    rowid like "%" || ? || "%" '''

    try:
        keuze1 = input('Kies nummer: ')
        rekening = cursor.execute(query, keuze1).fetchone()
        rekening = str(rekening)
        rekening = rekening[2:-3]
        return rekening
    except sqlite3.OperationalError:
        print()
        print('foutieve invoer probeer opnieuw')
        filescript_mammom.menu()

def geldstroom():
    
    rekening1=keuze()
    rekening2=keuze()
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    export=export_folder()
    try:
        data = cursor.execute('''select
    *
    from
    '''+rekening1+'''
	
	where
	'''+rekening1+'''.tegenrekening == '''"'"+rekening2+"'")
    
        with open(export+'geldstroom_'+rekening1+'_'+rekening2+'''.csv''', 'w+') as f:
            writer = csv.writer(f)
            writer.writerow(['Unieke_transactiecode', 'Rekeningnummer', 'Rekenhouder_of_tenaamgestelde', 'Pasvolgnummer', 'Transactiedatum', 'Tijdstip_transactie', 'Tijdzone_transactie', 'Boekdatum', 'Valutadatum', 'Type_transactie', 'BIC_code_transactie', 'Omschrijving', 'Transactiebedrag_Debet', 'Transactiebedrag_Credit', 'Valuta_transactie', 'Tegenrekening', 'Tenaamstelling_tegenrekening'])
            writer.writerows(data)
        filescript_mammom.menu()

    except sqlite3.OperationalError:
        print()
        print('fout in de zoekvraag probeer opnieuw')

        filescript_mammom.menu()

def buitenlandse_transacties():
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    
    rekening1=keuze()
    export=export_folder()




    try:
        data = cursor.execute('''select
    *
    from
    '''+rekening1+'''
	
	where
	'''+rekening1+'''.tegenrekening NOT LIKE ('NL%') and length(tegenrekening) > 10''' ) #de waarde in tegenrekening mag niet beginnen met NL en moet gevuld zijn met meer dan 10 karakters.
    

    
        with open(export+'buitenlandse_transacties_'+rekening1+'''.csv''', 'w+') as f:
            writer = csv.writer(f)
            writer.writerow(['Unieke_transactiecode', 'Rekeningnummer', 'Rekenhouder_of_tenaamgestelde', 'Pasvolgnummer', 'Transactiedatum', 'Tijdstip_transactie', 'Tijdzone_transactie', 'Boekdatum', 'Valutadatum', 'Type_transactie', 'BIC_code_transactie', 'Omschrijving', 'Transactiebedrag_Debet', 'Transactiebedrag_Credit', 'Valuta_transactie', 'Tegenrekening', 'Tenaamstelling_tegenrekening'])
            writer.writerows(data)

        
        filescript_mammom.menu()

    except sqlite3.OperationalError:
        print()
        print('fout in de zoekvraag probeer opnieuw')

        filescript_mammom.menu()

def top_ontvangen():
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    

    rekening1=keuze()
    export=export_folder()

    try:
        data = cursor.execute('''
        select
    *
    from '''
    +rekening1+'''
	 order by
	transactiebedrag_credit desc
	limit 10''')
     

    
        with open(export+'hoogste_ontvangen_'+rekening1+'''.csv''', 'w+') as f:
            writer = csv.writer(f)
            writer.writerow(['Unieke_transactiecode', 'Rekeningnummer', 'Rekenhouder_of_tenaamgestelde', 'Pasvolgnummer', 'Transactiedatum', 'Tijdstip_transactie', 'Tijdzone_transactie', 'Boekdatum', 'Valutadatum', 'Type_transactie', 'BIC_code_transactie', 'Omschrijving', 'Transactiebedrag_Debet', 'Transactiebedrag_Credit', 'Valuta_transactie', 'Tegenrekening', 'Tenaamstelling_tegenrekening'])
            writer.writerows(data)
        filescript_mammom.menu()

    except sqlite3.OperationalError:
        print()
        print('fout in de zoekvraag probeer opnieuw')

        filescript_mammom.menu()

def top_uitgaand():
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    

    rekening1=keuze()
    export=export_folder()

    try:
        data = cursor.execute('''
        select
    *
    from '''
    +rekening1+'''
	 order by
	transactiebedrag_debet desc
	limit 10''')
     

    
        with open(export+'hoogste_uitgaand_'+rekening1+'''.csv''', 'w+') as f:
            writer = csv.writer(f)
            writer.writerow(['Unieke_transactiecode', 'Rekeningnummer', 'Rekenhouder_of_tenaamgestelde', 'Pasvolgnummer', 'Transactiedatum', 'Tijdstip_transactie', 'Tijdzone_transactie', 'Boekdatum', 'Valutadatum', 'Type_transactie', 'BIC_code_transactie', 'Omschrijving', 'Transactiebedrag_Debet', 'Transactiebedrag_Credit', 'Valuta_transactie', 'Tegenrekening', 'Tenaamstelling_tegenrekening'])
            writer.writerows(data)
        filescript_mammom.menu()

    except sqlite3.OperationalError:
        print()
        print('fout in de zoekvraag probeer opnieuw')

        filescript_mammom.menu()

def export_folder():
    if platform.system() == 'Windows':
        export=('export\\')
    else:
        export=('export/')
    return export
