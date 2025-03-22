import sys,os,glob,psycopg2,csv,time, datetime, errno, json, collections
from pyexcel_xls import get_data
from pyexcel_xls import save_data
from datetime import datetime
from collections import OrderedDict
	
#---------------------------------------------------------------------------------------------
def exec_query ( str_query ):
	try:
		cur.execute(str_query) 
		print 'Wykonane zapytanie '+str_query
	except Exception, e:
		print '	Nie udalo sie wykonac zapytania:'+str_query
		err_log.write('Nie udalo sie wykonac zapytania (select):'+str_query+'\n')
		print e
	return cur
#---------------------------------------------------------------------------------------------
	
def exec_query_commit( str_query ):
	try:
		cur.execute(str_query) 
		conn_PG.commit()
		print 'Wykonane zapytanie '+str_query
	except Exception, e:
		print '	Nie udalo sie wykonac zapytania:'+str_query
		print e
		err_log.write('Nie udalo sie wykonac zapytania (commit):'+str_query+'\n')
		os.system('pause')
	return cur 	
#----------------------------------------------------------------------------------------------------------------------------------		

def get_data_from_ZAWIADOMIENIA():
	dirs_name = glob.glob ('zawiadomienia')
	#Stworzenie tablicy na dane z katalogu ZAWIADOMIENIA 
	drop_query = 'DROP TABLE IF EXISTS 	ZAWIADOMIENIA_listy'
	exec_query_commit(drop_query)
	
	create_query = 'create table ZAWIADOMIENIA_listy ( NR_ZAWIAD character varying(1500),S character varying(1500),DATA character varying(1500),GODZINA character varying(1500),G character varying(1500),DDD character varying(1500),KW character varying(1500),OBREB_NR character varying(1500),OBREB_NAZWA character varying(1500),WL character varying(1500),RODZICE character varying(1500),ADRES_CZ2 character varying(1500),ADRES_CZ1 character varying(1500),SPOTKANIE_GDZIE character varying(1500),SPOTKANIE_DZ character varying(1500),data_napisania character varying(1500),kerg character varying(1500),pwpg character varying(5000), plik_csv character varying(1500))'
	exec_query_commit(create_query)
			
	for dir_name in dirs_name:
		files_xls_names = glob.glob(dir_name+'\\*.xls*')
		print files_xls_names
		for file_xls_name in files_xls_names:
			print file_xls_name
			table_name = os.path.basename(file_xls_name)
			table_name = table_name.split('.')[0]
			data = get_data(file_xls_name)
			licznik = 1
			LP =''
			for wiersz in data['Arkusz1']:
				if licznik > 1 and len(wiersz) > 1: 
					NR_ZAWIAD = str(wiersz[0])
					S = str(wiersz[1].encode('utf8'))
					DATA = str(wiersz[2].encode('utf8'))
					GODZINA = str(wiersz[3].encode('utf8'))
					G = str(wiersz[4].encode('utf8'))
					DDD = str(wiersz[5].encode('utf8'))
					KW = str(wiersz[6].encode('utf8'))
					OBREB_NR = str(wiersz[7].encode('utf8'))
					OBREB_NAZWA = str(wiersz[8].encode('utf8'))
					WL = str(wiersz[9].encode('utf8'))
					RODZICE = str(wiersz[10].encode('utf8'))
					ADRES_CZ1 = str(wiersz[11].encode('utf8'))
					ADRES_CZ2 = str(wiersz[12].encode('utf8'))
					SPOTKANIE_GDZIE = str(wiersz[13].encode('utf8'))
					SPOTKANIE_DZ = str(wiersz[14].encode('utf8'))
					data_napisania = str(wiersz[15].encode('utf8'))
					kerg = str(wiersz[16].encode('utf8'))
					pwpg = str(wiersz[17].encode('utf8'))
					#zaladowania danych do bazy   
					insert_query = "insert into ZAWIADOMIENIA_listy  VALUES ('"+NR_ZAWIAD+"','"+S+"','"+DATA+"','"+GODZINA+"','"+G+"','"+DDD+"','"+KW+"','"+OBREB_NR+"','"+OBREB_NAZWA+"','"+WL+"','"+RODZICE+"','"+ADRES_CZ2+"','"+ADRES_CZ1+"','"+SPOTKANIE_GDZIE+"','"+SPOTKANIE_DZ+"','"+data_napisania+"','"+kerg+"','"+pwpg+"','"+table_name+"');"
					exec_query_commit(insert_query)	
				licznik = licznik+1

#--------------------------------------------------------------------------------------------------------------------------------------------
#-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
#--------------------------------------------------------------------------------------------------------------------------------------------
if not os.path.exists('.\\log'):
    try:
        os.makedirs('.\\log')
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise

naz_lik_log = time.strftime('%Y%m%d-%H_%M_%S')
naz_log = open('.\\log\\copy'+naz_lik_log+'.log', 'w')
err_log = open('.\\log\\ERR'+naz_lik_log+'.log', 'w')


#polacznie z baza POSTGRES 
teraz = time.asctime( time.localtime(time.time()))
try:
	conn_PG = psycopg2.connect("dbname='__db_name__' user='__user__' host='__host__' password='__passwd__'")
	naz_log.write(teraz + ' [INF] Polaczono z baza danych\n')
except:
	print 'I am unable to connect to the database'
	err_log.write(teraz +' - [ERR] Nie udalo sie naiazac polacznia z baz\n')

cur = conn_PG.cursor()
get_data_from_ZAWIADOMIENIA()

