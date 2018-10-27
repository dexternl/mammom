# script: queries_mammom.py
# analyseer database met verschillende queries
# creator: Erik-Jan de Kruijf
# datum: 14-10-2018

__version__ = 0.1

import sqlite3, csv, platform, os
import filescript_mammom


def geimporteerde_bestanden(): #Geef een overzicht van de geimporteerde bestanden
    
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
        print('\t{}'.format(row[0][7:]))
    connection.commit()
    connection.close()
    print()
    filescript_mammom.menu()

def lijst_rekeningnummers(): #Geef een overzicht van de geimporteerde bestanden
    schoon_scherm()
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

def lijst_rekeningnummers_zonder_menu(): #Geef een overzicht van de geimporteerde bestanden
    
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

def overeenkomstige_transacties(): #Kijk in de tegenrekening kolom of er rekeningnummers overeenkomen met de tegenrekening kolom van de andere rekening.
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    export=export_folder()
    schoon_scherm()

    rekening1=keuze()
    rekening2=keuze()

    try:
        data = cursor.execute('''SELECT distinct*  FROM '''+rekening1+'''
    INNER JOIN '''+rekening2+''' ON '''+rekening1+'''.Tegenrekening = '''+rekening2+'''.Tegenrekening
    where '''+rekening1+'''.Tegenrekening != "N/A"

    UNION

    SELECT distinct *  FROM '''+rekening2+'''
    INNER JOIN '''+rekening1+''' ON '''+rekening2+'''.Tegenrekening = '''+rekening1+'''.Tegenrekening
    where '''+rekening2+'''.Tegenrekening != "N/A";''')
    
        with open(export+'overeenkomsten_'+rekening1+'_'+rekening2+'''.csv''', 'w+') as f:
            writer = csv.writer(f)
            writer.writerow(['Unieke_transactiecode', 'Rekeningnummer', 'Rekenhouder_of_tenaamgestelde', 'Pasvolgnummer', 'Transactiedatum', 'Tijdstip_transactie', 'Tijdzone_transactie', 'Boekdatum', 'Valutadatum', 'Type_transactie', 'BIC_code_transactie', 'Omschrijving', 'Transactiebedrag_Debet', 'Transactiebedrag_Credit', 'Valuta_transactie', 'Tegenrekening', 'Tenaamstelling_tegenrekening'])
            writer.writerows(data)
        filescript_mammom.menu()

    except sqlite3.OperationalError:
        schoon_scherm()
        print()
        print('fout in de zoekvraag probeer opnieuw')

        filescript_mammom.menu()

def keuze(): #Genereer een lijst met geimporteerde rekeningnummers en vraag de gebruiker om een te kiezen voor verdere functies
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
        if keuze1 == "":
            schoon_scherm()
            print()
            print('vul aub een waarde in')
            filescript_mammom.menu()
        else:
            try:
                int(keuze1)
                rekening = cursor.execute(query, keuze1).fetchone()
                rekening = str(rekening)
                rekening = rekening[2:-3]
                return rekening
            except:
                schoon_scherm()
                print()
                print(keuze1+' is geen geldige keuze, probeer opnieuw')
                filescript_mammom.menu()
            
    except sqlite3.OperationalError:
        schoon_scherm()
        print()
        print('foutieve invoer probeer opnieuw')
        filescript_mammom.menu()

def geldstroom(): #Maak een overzicht van transacties tussen 2 gekozen rekeningnummers en exporteer de resultaten naar een csv
    
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

def buitenlandse_transacties(): #Maak een overzicht met transacties naar niet nederlandse rekeningnummers en exporteer resultaat naar een csv
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

def top_ontvangen(): #Maak een overzicht van de 10 transacties met de grootste waarde van ontvangen bedragen
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

def top_uitgaand(): #Maak een overzicht van de 10 transacties met de grootste waarde van uitgaande bedragen
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

def export_folder(): #controleer of systeem een windows of unix based systeem om zo te kijken welke kant de / uit moet gaan
    if platform.system() == 'Windows':
        export=('export\\')
    else:
        export=('export/')
    return export

def schoon_scherm(): #controleer of systeem een windows of unix based systeem is om zo te kijken hoe het sche
    if platform.system() == 'Windows':
        scherm= os.system("cls")
    else:
        schem= os.system("clear")
    


    
    
