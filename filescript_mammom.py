# script: importeer_mammom.py
# instancieer database en importeer data
# creator: Erik-Jan de Kruijf
# datum: 9-10-2018

__version__ = 0.1

import csv, sqlite3, os, platform
import queries_mammom

def filecheck(): # Probeer de database file te openen, waarschuw de gebruiker als er nog data in de database staat. 
    try:
        if os.path.getsize('mammom.sqlite') >= 16385:
            print()
            print('\t\t Waarschuwing! Er staat mogelijk nog data in de database!')
        else:
            with open('mammom.sqlite') as file:             
                connection = sqlite3.connect('mammom.sqlite')     
                cursor = connection.cursor()
            
                try:
                    cursor.execute("CREATE TABLE rekeningnummers (Rekeningnummer, tenaamstelling);")
                    cursor.execute("CREATE TABLE csvbestanden (CSVbestanden BLOB(255));")
                    cursor.execute("create table relaties(bron,doel)")
                    connection.commit()
                    connection.close()
                except:
                    pass

    except FileNotFoundError:
        with open('mammom.sqlite',"w") as file:         #Als de database nog niet bestaat maak hem dan aan
            connection = sqlite3.connect('mammom.sqlite')
            cursor = connection.cursor()
            cursor.execute("CREATE TABLE rekeningnummers (Rekeningnummer, tenaamstelling);")
            cursor.execute("CREATE TABLE csvbestanden (CSVbestanden BLOB(255));")
            cursor.execute("create table relaties(bron,doel)")
            connection.commit()
            connection.close()

def bestaat_bestand(filename): #controleer of bestand gekozen in de import functie bestaat
    
    try:
        with open(filename) as csvfile:
            csvfile.readline()
        controleer_csv(filename)
    except FileNotFoundError:
        print()
        print('Bestand ' +filename+ ' bestaat niet probeer aub opnieuw')
        print()
        menu()

def controleer_csv(filename): #controleer of de csv geimporteerd kan worden.
    
    controlelijn = ""
    with open(filename) as csvfile:
        for i in range(17):
            csvfile.readline()
        controlelijn = csvfile.readline().strip().split(';')[0]
    if controlelijn == "Unieke_transactiecode":
        controleer_reeds_geimporteerd(filename)
    else:
        print()
        print("bestand " +filename+ " kan niet worden geimporteerd")
        print()
        menu()

def controleer_csv_bulk(filename): #controleer of de csv geimporteerd kan worden in bulk mode. Hier wordt bij een fout geen menu aangeroepen
    
    controlelijn = ""
    with open(filename) as csvfile:
        for i in range(17):
            csvfile.readline()
        controlelijn = csvfile.readline().strip().split(';')[0]
    if controlelijn == "Unieke_transactiecode":
        controleer_reeds_geimporteerd_bulk(filename)
    else:
        
        print("bestand " +filename+ " kan niet worden geimporteerd")
        
    
def controleer_reeds_geimporteerd_bulk(filename): #controleer of de csv reeds geimporteerd is in bulk mode. Hier wordt bij een fout geen menu aangeroepen
    csvbestand = '"' + filename + '"'
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()

    cursor.execute("select csvbestanden from csvbestanden where csvbestanden = "+csvbestand)
    result=cursor.fetchone()
    connection.close()
    result = str(result)
    result = result[2:-3]
    if result == filename:
        print("bestand " +filename+ " is al eens geimporteerd")
    else:
        bepaal_rekeningnummer(filename)

def controleer_reeds_geimporteerd(filename): #controleer of de csv geimporteerd kan worden. 
    csvbestand = '"' + filename + '"'
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()

    cursor.execute("select csvbestanden from csvbestanden where csvbestanden = "+csvbestand)
    result=cursor.fetchone()
    connection.close()
    result = str(result)
    result = result[2:-3]
    if result == filename:
        print()
        print("bestand " +filename+ " is al eens geimporteerd")
        menu()
    else:
        bepaal_rekeningnummer(filename)
    
def bepaal_rekeningnummer(filename): #bepaal het rekeningnummer en tenaamstelling van de te importeren csv
    rekeningnummer = ""

    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    with open(filename) as csvfile:
        for i in range(18):
            csvfile.readline()
        rekeningnummer = csvfile.readline().strip().split(';')[1]
        tenaamstelling = csvfile.readline().strip().split(';')[2]
        connection.commit()
    try:
        query = "Create table " +rekeningnummer+ "(Unieke_transactiecode, Rekeningnummer, Rekenhouder_of_tenaamgestelde, Pasvolgnummer, Transactiedatum, Tijdstip_transactie, Tijdzone_transactie, Boekdatum, Valutadatum, Type_transactie, BIC_code_transactie, Omschrijving, Transactiebedrag_Debet, Transactiebedrag_Credit, Valuta_transactie, Tegenrekening, Tenaamstelling_tegenrekening)"
        cursor.execute(query)

        connection.commit()
        connection.close()
        import_csv(filename,rekeningnummer, tenaamstelling)
    except:
        import_csv(filename,rekeningnummer, tenaamstelling)
    
def import_csv(filename, rekeningnummer, tenaamstelling): #importeer de csv en plaats het rekeningnummer en tenaamstelling in de rekeningnummers tabel
    query = "INSERT INTO " +rekeningnummer+ "(Unieke_transactiecode, Rekeningnummer, Rekenhouder_of_tenaamgestelde, Pasvolgnummer, Transactiedatum, Tijdstip_transactie, Tijdzone_transactie, Boekdatum, Valutadatum, Type_transactie, BIC_code_transactie, Omschrijving, Transactiebedrag_Debet, Transactiebedrag_Credit, Valuta_transactie, Tegenrekening, Tenaamstelling_tegenrekening) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    rekeningnummer_import = '"' + rekeningnummer + '"'
    tenaamstelling_import = '"' + tenaamstelling + '"'
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()

 

    with open(filename) as csvfile: 
        for i in range(18):
            csvfile.readline()

        for line in csvfile:
            fields = line.strip().split(';')
            cursor.execute(query, fields)
    
    cursor.execute ("insert into rekeningnummers (rekeningnummer, tenaamstelling) values ("+rekeningnummer_import+","+tenaamstelling_import+")")
    connection.commit()
    connection.close()
    os.rename(filename,filename+'_done')
    vul_csvbestandtabel(filename,rekeningnummer)
      
def vul_csvbestandtabel(filename,rekeningnummer): #plaats geimporteerde csv in csvbestandentabel
    csvbestand = '"' + filename + '"'
    rekeningnummer = '"' + rekeningnummer + '"'
    connection = sqlite3.connect('mammom.sqlite')
    cursor = connection.cursor()
    cursor.execute("insert into csvbestanden (csvbestanden) values ("+csvbestand+")")
    cursor.execute("insert into relaties(bron, doel) select distinct tegenrekening, rekeningnummer from " +rekeningnummer+" where Transactiebedrag_credit is not '""' and Tegenrekening is not '"'N/A'"'")
    cursor.execute("insert into relaties(bron, doel) select distinct Rekeningnummer, Tegenrekening from " +rekeningnummer+" where Transactiebedrag_debet is not '""' and Tegenrekening is not '"'N/A'"'")
    #queries_mammom.vul_relaties(rekeningnummer)
    connection.commit()
    connection.close()

def import_csv_all(): #importeer csv in bulk. Hierbij pakt hij alle csv bestanden in de import map
    for file in os.listdir("import"):
        if file.endswith(".csv"):
            if platform.system() == 'Windows':
                bestaat_bestand=('import\\' + file)
            else:
                bestaat_bestand=('import/' + file)
            
            controleer_csv_bulk(bestaat_bestand)
    menu()
            
def drop_db(): #Wis database bestand en maak een lege opnieuw aan.
    choice = input(""" Weet je zeker dat de database opgeschoond mag worden? Dit kan niet ongedaan worden gemaakt!
                      J: Ja
                      N: Nee
                      

                      Maak uw keuze: """)

    if choice == "J":
        os.remove("mammom.sqlite")
        filecheck()
        print("DATABASE OPGESCHOOND")
        menu()
    
    else:
        print("Opschoning geannuleerd")
        menu()
    
def lijst_geexporteerd(): #genereer lijst met geexporteerde csv bestanden
    for file in os.listdir("export"):
        print(file)
    menu()    

def menu(): #Hoofdmenu
    print()
    print("\t\t\t************MENU**************")
    print()

    choice = input("""
                      A: Import CSV
                      B: Import alle CSV bestanden uit de import map

                      C: Welke bestanden zijn al geimporteerd
                      D: Welke rekeningnummers zijn geimporteerd

                      E: zoek overeenkomstige 3e partij en exporteer resultaat naar csv
                      F: Geldstroom tussen 2 rekeningnummers
                      G: Transacties met buitenlandse rekeningnummers
                      H: Top 10 transacties ontvangen
                      I: Top 10 transacties uitgaand

                      L: Lijst van geexporteerde bestanden
                      W: Wis Database
                      X: Afsluiten

                      Maak uw keuze: """)
    if choice == "A" or choice =="a":
        filename = input("Voer bestandsnaam in: ")
        bestaat_bestand(filename)
            
    if choice == "B" or choice =="b":
        import_csv_all()
        print()
    
    if choice == "C" or choice =="c":
        queries_mammom.geimporteerde_bestanden()
        print()
    
    if choice == "D" or choice =="d":
        queries_mammom.lijst_rekeningnummers()
        print()
    
    if choice == "E" or choice =="e":
        queries_mammom.lijst_rekeningnummers_zonder_menu()
        print()
        queries_mammom.overeenkomstige_transacties()

    if choice == "F" or choice =="f":
        queries_mammom.lijst_rekeningnummers_zonder_menu()
        print()
        queries_mammom.geldstroom()
    
    if choice == "G" or choice =="g":
        queries_mammom.lijst_rekeningnummers_zonder_menu()
        print()
        queries_mammom.buitenlandse_transacties()
    
    if choice == "H" or choice =="h":
        queries_mammom.lijst_rekeningnummers_zonder_menu()
        print()
        queries_mammom.top_ontvangen()

    if choice == "I" or choice =="i":
        queries_mammom.lijst_rekeningnummers_zonder_menu()
        print()
        queries_mammom.top_uitgaand()

    if choice == "L" or choice == "l":
        print()
        lijst_geexporteerd()

    if choice == "W" or choice =="w":
       drop_db()

    if choice == "x" or choice == "X":
        exit()
    else:
        print()
        print('\tOngeldige keuze')
        menu()
    
