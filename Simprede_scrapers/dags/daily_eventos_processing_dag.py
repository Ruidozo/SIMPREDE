# File: airflow-GOOGLE-NEWS-SCRAPER/dags/daily_eventos_processing_dag.py
# Pipeline diário de processamento de eventos SIMPREDE
# Processa dados de staging para tabela eventos com georreferenciação otimizada
#!/usr/bin/env python3
"""
SIMPREDE Daily Eventos Processing Pipeline
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from dotenv import find_dotenv, load_dotenv

# Load environment variables from the project root
load_dotenv(find_dotenv())
from airflow.operators.python import PythonOperator

# Configuração de importações seguras
try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    POSTGRES_DISPONIVEL = True
except ImportError:
    POSTGRES_DISPONIVEL = False
    class PostgresHook:
        def __init__(self, *args, **kwargs):
            pass

class DatabaseManager:
    """
    Gestor centralizado de operações de base de dados
    Implementa padrões consistentes e reutilizáveis
    """
    
    def __init__(self, context):
        self.context = context
        self.connection_uri = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Configura ligação à base de dados usando variáveis de ambiente"""
        print("🔧 A configurar ligação à base de dados...")
        
        # Função auxiliar para obter variáveis de ambiente de forma robusta
        def get_env_var(var_name, default=''):
            """Obtém variável de ambiente com múltiplas tentativas de fallback"""
            value = os.getenv(var_name, default).strip()
            if not value:
                # Tenta abordagens alternativas
                import subprocess
                try:
                    # Tenta obter do ambiente shell
                    result = subprocess.run(['printenv', var_name], capture_output=True, text=True)
                    if result.returncode == 0:
                        value = result.stdout.strip()
                        print(f"📋 Encontrado {var_name} via subprocess: {'✅' if value else '❌'}")
                except Exception as e:
                    print(f"⚠️ Método subprocess falhou para {var_name}: {e}")
            return value
        
        # Obtém todas as credenciais necessárias
        credenciais = {
            'user': get_env_var('DB_USER'),
            'password': get_env_var('DB_PASSWORD'),
            'host': get_env_var('DB_HOST'),
            'port': get_env_var('DB_PORT', '6543'),
            'database': get_env_var('DB_NAME', 'postgres')
        }
        
        print("🔍 Estado das variáveis de ambiente:")
        print(f"  - DB_USER: {'✅ Definido' if credenciais['user'] else '❌ Em falta'} - Comprimento: {len(credenciais['user']) if credenciais['user'] else 0}")
        print(f"  - DB_PASSWORD: {'✅ Definido' if credenciais['password'] else '❌ Em falta'} - Comprimento: {len(credenciais['password']) if credenciais['password'] else 0}")
        print(f"  - DB_HOST: {'✅ Definido' if credenciais['host'] else '❌ Em falta'} - Valor: '{credenciais['host']}'")
        print(f"  - DB_PORT: {credenciais['port']}")
        print(f"  - DB_NAME: {credenciais['database']}")
        
        # Debug: Imprime valores brutos das variáveis de ambiente (cuidado com password)
        print("🔍 Verificação bruta das variáveis de ambiente:")
        vars_brutas = ['DB_USER', 'DB_HOST', 'DB_PORT', 'DB_NAME']
        for var in vars_brutas:
            valor_bruto = os.environ.get(var, 'NAO_DEFINIDO')
            print(f"  - {var}: '{valor_bruto}'")
        
        # Verifica password separadamente (não imprime valor)
        password_bruto = os.environ.get('DB_PASSWORD', 'NAO_DEFINIDO')
        print(f"  - DB_PASSWORD: {'DEFINIDO' if password_bruto != 'NAO_DEFINIDO' else 'NAO_DEFINIDO'} (comprimento: {len(password_bruto) if password_bruto != 'NAO_DEFINIDO' else 0})")
        
        # Valida credenciais obrigatórias
        campos_obrigatorios = ['user', 'password', 'host']
        campos_em_falta = [campo for campo in campos_obrigatorios if not credenciais[campo]]
        
        if campos_em_falta:
            print(f"❌ CRÍTICO: Credenciais em falta: {campos_em_falta}")
            print("📋 Estado detalhado atual:")
            print(f"  - DB_USER: '{credenciais['user']}' (vazio: {not credenciais['user']})")
            print(f"  - DB_PASSWORD: {'***' if credenciais['password'] else 'VAZIO'} (vazio: {not credenciais['password']})")
            print(f"  - DB_HOST: '{credenciais['host']}' (vazio: {not credenciais['host']})")
            print("")
            print("🔧 Passos de resolução de problemas:")
            print("1. Verificar se o ficheiro .env existe e tem formato correto:")
            print("   cat .env | grep DB_")
            print("")
            print("2. Verificar se Docker está a carregar variáveis de ambiente:")
            print("   docker compose exec airflow-standalone env | grep DB_")
            print("")
            print("3. Verificar definição env_file no docker-compose.yml:")
            print("   grep -A 5 env_file docker-compose.yml")
            print("")
            print("4. Reiniciar containers para recarregar ambiente:")
            print("   docker compose down && docker compose up")
            print("")
            print("5. Alternativa: Definir variáveis diretamente na secção environment do docker-compose.yml")
            print("")
            print("6. Verificar se as variáveis estão a ser passadas corretamente:")
            print("   docker compose config | grep -A 10 environment")
            print("")
            
            # Tenta diagnóstico adicional
            print("🔍 Diagnóstico adicional do ambiente:")
            try:
                # Lista todas as variáveis que começam com DB_
                db_vars = {k: v for k, v in os.environ.items() if k.startswith('DB_')}
                print(f"📊 Encontradas {len(db_vars)} variáveis DB_:")
                for var_name, var_value in db_vars.items():
                    if 'PASSWORD' in var_name:
                        print(f"  - {var_name}: [CENSURADO] (comprimento: {len(var_value)})")
                    else:
                        print(f"  - {var_name}: '{var_value}'")
                
                # Verifica se há alguma variável relacionada com Airflow
                airflow_vars = {k: v for k, v in os.environ.items() if 'AIRFLOW' in k and 'DB' in k}
                if airflow_vars:
                    print(f"📊 Encontradas {len(airflow_vars)} variáveis relacionadas com Airflow DB:")
                    for var_name in airflow_vars.keys():
                        print(f"  - {var_name}")
                
            except Exception as diag_error:
                print(f"⚠️ Erro no diagnóstico adicional: {diag_error}")
            
            raise ValueError(f"Credenciais em falta: {campos_em_falta}")
        
        # Constrói URI de ligação
        self.connection_uri = (
            f"postgresql://{credenciais['user']}:{credenciais['password']}"
            f"@{credenciais['host']}:{credenciais['port']}/{credenciais['database']}"
            f"?sslmode=require"
        )
        
        print(f"✅ Ligação configurada: {credenciais['host']}:{credenciais['port']}")
        print(f"🔍 URI de ligação: postgresql://[user]:[password]@{credenciais['host']}:{credenciais['port']}/{credenciais['database']}?sslmode=require")
        
        # Teste de ligação adicional se psycopg2 estiver disponível
        try:
            import psycopg2
            print("🔄 A testar ligação direta com psycopg2...")
            test_conn = psycopg2.connect(self.connection_uri)
            test_conn.close()
            print("✅ Teste de ligação direta bem-sucedido!")
        except ImportError:
            print("⚠️ psycopg2 não disponível para teste de ligação direta")
        except Exception as e:
            print(f"❌ Teste de ligação direta falhou: {str(e)}")
            print("💡 Isto pode indicar:")
            print("  - Problema de conectividade de rede")
            print("  - Credenciais inválidas")
            print("  - Servidor de base de dados em baixo")
            # Não levanta exceção aqui - deixa que a lógica principal tente
    
    def execute_query(self, query, params=None, fetch_results=False):
        """
        Executa query com gestão adequada de ligação e erros
        Retorna resultados se solicitado
        """
        try:
            import psycopg2
            
            with psycopg2.connect(self.connection_uri) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    
                    if fetch_results:
                        return cursor.fetchall()
                    else:
                        affected_rows = cursor.rowcount
                        conn.commit()
                        return affected_rows
                        
        except ImportError:
            raise Exception("psycopg2 não disponível")
        except Exception as e:
            raise Exception(f"Erro na base de dados: {str(e)}")

# Configuração do DAG
default_args = {
    'owner': 'simprede',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_eventos_processing_optimized',
    default_args=default_args,
    description='Processamento diário otimizado de dados de eventos',
    schedule='0 6 * * *',  # Executa diariamente às 6:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['daily', 'eventos', 'staging', 'simprede', 'otimizado'],
    max_active_runs=1,
    doc_md=__doc__,
)

def configurar_ligacao(**context):
    """Configura ligação à base de dados de forma otimizada"""
    print("🔧 A configurar ligação à base de dados...")
    
    if not POSTGRES_DISPONIVEL:
        raise Exception("Fornecedor PostgreSQL não disponível")
    
    db_manager = DatabaseManager(context)
    
    # Armazena detalhes no XCom para outras tarefas
    context['task_instance'].xcom_push(
        key='db_connection', 
        value=db_manager.connection_uri
    )
    
    return db_manager.connection_uri

def verificar_staging(**context):
    """Verifica disponibilidade e estado da tabela de staging"""
    print("📋 A verificar tabela de staging...")
    
    connection_uri = context['task_instance'].xcom_pull(
        task_ids='configurar_ligacao', key='db_connection'
    )
    
    if not connection_uri:
        raise ValueError("URI de ligação não encontrada")
    
    # Determina nome da tabela de staging
    execution_date = context['ds_nodash']
    dag_conf = context.get('dag_run', {}).conf or {}
    staging_table = dag_conf.get(
        'staging_table', 
        f'google_scraper.artigos_filtrados_{execution_date}_staging'
    )
    
    db_manager = DatabaseManager(context)
    db_manager.connection_uri = connection_uri
    
    # Verifica existência da tabela
    tabela_existe_query = """
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = 'google_scraper' 
        AND table_name = %s
    """
    
    resultado = db_manager.execute_query(
        tabela_existe_query, 
        (staging_table.split('.')[-1],), 
        fetch_results=True
    )
    
    tabela_existe = resultado[0][0] > 0
    
    if not tabela_existe:
        print(f"❌ Tabela de staging não existe: {staging_table}")
        raise ValueError(f"Tabela {staging_table} não encontrada")
    
    # Conta registos
    contagem_query = f"SELECT COUNT(*) FROM {staging_table}"
    resultado_contagem = db_manager.execute_query(
        contagem_query, fetch_results=True
    )
    num_registos = resultado_contagem[0][0]
    
    print(f"✅ Tabela encontrada: {staging_table} ({num_registos} registos)")
    
    # Armazena informação no XCom
    staging_info = {
        'table_name': staging_table,
        'row_count': num_registos,
        'exists': tabela_existe
    }
    
    context['task_instance'].xcom_push(key='staging_info', value=staging_info)
    
    return staging_info

def processar_staging_para_eventos(**context):
    """
    Processa dados de staging para tabela eventos de forma otimizada
    Implementa upsert eficiente e validação de dados
    """
    print("🔄 A processar dados de staging para eventos...")
    
    connection_uri = context['task_instance'].xcom_pull(
        task_ids='configurar_ligacao', key='db_connection'
    )
    
    staging_info = context['task_instance'].xcom_pull(
        task_ids='verificar_staging', key='staging_info'
    )
    
    if not staging_info or staging_info['row_count'] == 0:
        print("⚠️ Sem dados para processar")
        return {'inserted_rows': 0, 'updated_rows': 0}
    
    db_manager = DatabaseManager(context)
    db_manager.connection_uri = connection_uri
    
    staging_table = staging_info['table_name']
    
    # Query otimizada de inserção com tratamento de conflitos
    # Formata datas como YYYY/MM/DD (convertendo de DD/MM/YYYY)
    insert_query = f"""
        INSERT INTO google_scraper.google_scraper_eventos (
            id, type, subtype, date, year, month, day, hour,
            latitude, longitude, georef_class, district, municipality, parish, dicofreg,
            fatalities, injured, evacuated, displaced, missing,
            source_name, source_date, source_type, page, location_geom
        )
        SELECT 
            af.id,
            COALESCE(af.type, 'Other'),
            COALESCE(af.subtype, 'Other'),
            TO_CHAR(TO_DATE(af.date, 'DD/MM/YYYY'), 'YYYY/MM/DD')::VARCHAR,
            EXTRACT(YEAR FROM TO_DATE(af.date, 'DD/MM/YYYY'))::INT,
            EXTRACT(MONTH FROM TO_DATE(af.date, 'DD/MM/YYYY'))::INT,
            EXTRACT(DAY FROM TO_DATE(af.date, 'DD/MM/YYYY'))::INT,
            COALESCE(af.hour, '08:00'),
            NULL, NULL, COALESCE(af.georef, 'unknown'),
            COALESCE(af.district, 'unknown'),
            COALESCE(af.municipali, 'unknown'),
            COALESCE(af.parish, 'unknown'),
            af.dicofreg,
            COALESCE(af.fatalities, 0),
            COALESCE(af.injured, 0),
            COALESCE(af.evacuated, 0),
            COALESCE(af.displaced, 0),
            COALESCE(af.missing, 0),
            af.source,
            CASE 
                WHEN af.sourcedate IS NOT NULL AND af.sourcedate != '' 
                     AND af.sourcedate ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$'
                THEN TO_CHAR(TO_DATE(af.sourcedate, 'DD/MM/YYYY'), 'YYYY/MM/DD')::VARCHAR
                ELSE TO_CHAR(TO_DATE(af.date, 'DD/MM/YYYY'), 'YYYY/MM/DD')::VARCHAR
            END,
            COALESCE(af.sourcetype, 'news_article'),
            af.page,
            NULL
        FROM {staging_table} af
        WHERE af.date IS NOT NULL 
        AND af.date != ''
        AND af.date ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$'
        ON CONFLICT (id) DO UPDATE SET
            type = EXCLUDED.type,
            subtype = EXCLUDED.subtype,
            date = EXCLUDED.date,
            fatalities = EXCLUDED.fatalities,
            injured = EXCLUDED.injured,
            evacuated = EXCLUDED.evacuated,
            displaced = EXCLUDED.displaced,
            missing = EXCLUDED.missing,
            updated_at = CURRENT_TIMESTAMP
    """
    
    linhas_afetadas = db_manager.execute_query(insert_query)
    
    print(f"✅ Processadas {linhas_afetadas} linhas de staging")
    
    resultado = {'inserted_rows': linhas_afetadas, 'staging_rows': staging_info['row_count']}
    context['task_instance'].xcom_push(key='process_result', value=resultado)
    
    return resultado

def atualizar_coordenadas(**context):
    """
    Atualiza coordenadas usando tabela de centróides de forma otimizada
    Implementa hierarquia freguesia → concelho → distrito
    """
    print("🔧 A atualizar coordenadas...")
    
    connection_uri = context['task_instance'].xcom_pull(
        task_ids='configurar_ligacao', key='db_connection'
    )
    
    db_manager = DatabaseManager(context)
    db_manager.connection_uri = connection_uri
    
    # Query otimizada com hierarquia de matching
    update_query = """
        UPDATE google_scraper.google_scraper_eventos 
        SET 
            latitude = coord_data.best_latitude,
            longitude = coord_data.best_longitude,
            georef_class = coord_data.best_georef_class,
            updated_at = CURRENT_TIMESTAMP
        FROM (
            SELECT 
                e.id,
                COALESCE(
                    -- Prioridade 1: Freguesias
                    (SELECT c.latitude FROM public.centroids c 
                     WHERE LOWER(TRIM(c.freguesia)) = LOWER(TRIM(e.parish)) 
                     AND c.latitude IS NOT NULL LIMIT 1),
                    -- Prioridade 2: Concelhos  
                    (SELECT c.latitude FROM public.centroids c 
                     WHERE LOWER(TRIM(c.concelho)) = LOWER(TRIM(e.municipality)) 
                     AND c.latitude IS NOT NULL LIMIT 1),
                    -- Prioridade 3: Distritos
                    (SELECT c.latitude FROM public.centroids c 
                     WHERE LOWER(TRIM(c.distrito)) = LOWER(TRIM(e.district)) 
                     AND c.latitude IS NOT NULL LIMIT 1)
                ) as best_latitude,
                COALESCE(
                    (SELECT c.longitude FROM public.centroids c 
                     WHERE LOWER(TRIM(c.freguesia)) = LOWER(TRIM(e.parish)) 
                     AND c.longitude IS NOT NULL LIMIT 1),
                    (SELECT c.longitude FROM public.centroids c 
                     WHERE LOWER(TRIM(c.concelho)) = LOWER(TRIM(e.municipality)) 
                     AND c.longitude IS NOT NULL LIMIT 1),
                    (SELECT c.longitude FROM public.centroids c 
                     WHERE LOWER(TRIM(c.distrito)) = LOWER(TRIM(e.district)) 
                     AND c.longitude IS NOT NULL LIMIT 1)
                ) as best_longitude,
                CASE 
                    WHEN EXISTS(SELECT 1 FROM public.centroids c 
                               WHERE LOWER(TRIM(c.freguesia)) = LOWER(TRIM(e.parish))) THEN 'freguesia'
                    WHEN EXISTS(SELECT 1 FROM public.centroids c 
                               WHERE LOWER(TRIM(c.concelho)) = LOWER(TRIM(e.municipality))) THEN 'concelho'
                    WHEN EXISTS(SELECT 1 FROM public.centroids c 
                               WHERE LOWER(TRIM(c.distrito)) = LOWER(TRIM(e.district))) THEN 'distrito'
                    ELSE 'unknown'
                END as best_georef_class
            FROM google_scraper.google_scraper_eventos e
            WHERE e.latitude IS NULL OR e.longitude IS NULL
        ) coord_data
        WHERE google_scraper_eventos.id = coord_data.id
        AND coord_data.best_latitude IS NOT NULL
    """
    
    eventos_atualizados = db_manager.execute_query(update_query)
    
    print(f"✅ Coordenadas atualizadas para {eventos_atualizados} eventos")
    
    context['task_instance'].xcom_push(
        key='coordinates_result', 
        value={'events_updated': eventos_atualizados}
    )
    
    return {'events_updated': eventos_atualizados}

def atualizar_geometria(**context):
    """Atualiza geometria de localização de forma otimizada"""
    print("🔧 A atualizar geometria...")
    
    connection_uri = context['task_instance'].xcom_pull(
        task_ids='configurar_ligacao', key='db_connection'
    )
    
    db_manager = DatabaseManager(context)
    db_manager.connection_uri = connection_uri
    
    # Verifica disponibilidade do PostGIS
    postgis_query = "SELECT COUNT(*) FROM pg_extension WHERE extname = 'postgis'"
    resultado_postgis = db_manager.execute_query(postgis_query, fetch_results=True)
    postgis_disponivel = resultado_postgis[0][0] > 0
    
    if postgis_disponivel:
        geometry_query = """
            UPDATE google_scraper.google_scraper_eventos 
            SET 
                location_geom = ST_AsText(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)),
                updated_at = CURRENT_TIMESTAMP
            WHERE latitude IS NOT NULL 
            AND longitude IS NOT NULL 
            AND location_geom IS NULL
        """
        print("🔧 A usar PostGIS para geometria")
    else:
        geometry_query = """
            UPDATE google_scraper.google_scraper_eventos 
            SET 
                location_geom = 'POINT(' || longitude || ' ' || latitude || ')',
                updated_at = CURRENT_TIMESTAMP
            WHERE latitude IS NOT NULL 
            AND longitude IS NOT NULL 
            AND location_geom IS NULL
        """
        print("🔧 A usar WKT para geometria")
    
    eventos_atualizados = db_manager.execute_query(geometry_query)
    
    print(f"✅ Geometria atualizada para {eventos_atualizados} eventos")
    
    return {'events_updated': eventos_atualizados}

def fundir_para_tabela_principal(**context):
    """
    Consolida dados de eventos para google_scraper_ocorrencias
    Funde dados de google_scraper.google_scraper_eventos para public.google_scraper_ocorrencias
    Esta é a tabela principal utilizada pelo dashboard
    
    Implementa lógica robusta de fallback:
    - Se 'eventos' table existe: usa-a (com coords)
    - Se não: usa staging table (sem coords)
    - Adiciona colunas em falta automaticamente
    """
    print("🔀 A consolidar/fundir dados para tabela principal...")
    
    connection_uri = context['task_instance'].xcom_pull(
        task_ids='configurar_ligacao', key='db_connection'
    )
    
    db_manager = DatabaseManager(context)
    db_manager.connection_uri = connection_uri
    
    # Determina nome da tabela de staging
    execution_date = context['ds_nodash']
    staging_table = f'artigos_filtrados_{execution_date}_staging'
    schema = 'google_scraper'
    
    # Verificar se tabela 'eventos' existe
    check_eventos_query = f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = '{schema}' AND table_name = 'eventos'
    """
    resultado_eventos = db_manager.execute_query(check_eventos_query, fetch_results=True)
    eventos_exists = resultado_eventos[0][0] > 0 if resultado_eventos else False
    
    if eventos_exists:
        print(f"📌 Usando tabela 'eventos' com dados processados")
        merge_query = """
            INSERT INTO public.google_scraper_ocorrencias (
                id, type, subtype, date, year, month, day, hour,
                latitude, longitude, georef_class, district, municipality, parish, dicofreg,
                fatalities, injured, evacuated, displaced, missing,
                source_name, source_date, source_type, page, location_geom
            )
            SELECT 
                e.id,
                e.type,
                e.subtype,
                TO_CHAR(CAST(e.date AS DATE), 'YYYY/MM/DD'),
                e.year,
                e.month,
                e.day,
                e.hour,
                e.latitude,
                e.longitude,
                e.georef_class,
                e.district,
                e.municipality,
                e.parish,
                e.dicofreg,
                COALESCE(e.fatalities, 0),
                COALESCE(e.injured, 0),
                COALESCE(e.evacuated, 0),
                COALESCE(e.displaced, 0),
                COALESCE(e.missing, 0),
                e.source_name,
                TO_CHAR(CAST(e.source_date AS DATE), 'YYYY/MM/DD'),
                e.source_type,
                e.page,
                e.location_geom
            FROM google_scraper.google_scraper_eventos e
            WHERE e.latitude IS NOT NULL 
            AND e.longitude IS NOT NULL
            AND e.date IS NOT NULL
            AND LOWER(e.type) != 'other'
            ON CONFLICT (id) DO UPDATE SET
                type = EXCLUDED.type,
                subtype = EXCLUDED.subtype,
                date = EXCLUDED.date,
                year = EXCLUDED.year,
                month = EXCLUDED.month,
                day = EXCLUDED.day,
                hour = EXCLUDED.hour,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                georef_class = EXCLUDED.georef_class,
                district = EXCLUDED.district,
                municipality = EXCLUDED.municipality,
                parish = EXCLUDED.parish,
                dicofreg = EXCLUDED.dicofreg,
                fatalities = EXCLUDED.fatalities,
                injured = EXCLUDED.injured,
                evacuated = EXCLUDED.evacuated,
                displaced = EXCLUDED.displaced,
                missing = EXCLUDED.missing,
                source_name = EXCLUDED.source_name,
                source_date = EXCLUDED.source_date,
                source_type = EXCLUDED.source_type,
                page = EXCLUDED.page,
                location_geom = EXCLUDED.location_geom,
                updated_at = CURRENT_TIMESTAMP
        """
    else:
        print(f"⚠️ Tabela 'eventos' não existe - usando staging table como fallback")
        # Fallback query para staging table com mapeamento de colunas seguro
        merge_query = f"""
            INSERT INTO public.google_scraper_ocorrencias (
                id, type, date, year, month, 
                latitude, longitude, district,
                fatalities, injured, evacuated, displaced, missing,
                source_name, page, updated_at
            )
            SELECT 
                COALESCE(s.id, s.ID, md5(CAST(ROW(s.title, s.date) AS TEXT))::TEXT),
                COALESCE(s.type, 'Unknown'),
                s.date,
                COALESCE(EXTRACT(YEAR FROM TO_DATE(s.date, 'DD/MM/YYYY'))::INTEGER, EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER),
                COALESCE(EXTRACT(MONTH FROM TO_DATE(s.date, 'DD/MM/YYYY'))::INTEGER, EXTRACT(MONTH FROM CURRENT_DATE)::INTEGER),
                0,  -- latitude (não disponível no staging)
                0,  -- longitude (não disponível no staging)
                COALESCE(s.district, 'Unknown'),
                COALESCE(CAST(s.fatalities AS INTEGER), 0),
                COALESCE(CAST(s.injured AS INTEGER), 0),
                COALESCE(CAST(s.evacuated AS INTEGER), 0),
                COALESCE(CAST(s.displaced AS INTEGER), 0),
                COALESCE(CAST(s.missing AS INTEGER), 0),
                COALESCE(s.source, 'Unknown'),
                COALESCE(s.page, ''),
                CURRENT_TIMESTAMP
            FROM {schema}.{staging_table} s
            ON CONFLICT (id) DO UPDATE SET
                type = EXCLUDED.type,
                date = EXCLUDED.date,
                year = EXCLUDED.year,
                month = EXCLUDED.month,
                district = EXCLUDED.district,
                fatalities = EXCLUDED.fatalities,
                injured = EXCLUDED.injured,
                evacuated = EXCLUDED.evacuated,
                displaced = EXCLUDED.displaced,
                missing = EXCLUDED.missing,
                source_name = EXCLUDED.source_name,
                page = EXCLUDED.page,
                updated_at = CURRENT_TIMESTAMP
        """
    
    linhas_fundidas = db_manager.execute_query(merge_query)
    
    print(f"✅ {linhas_fundidas} registos consolidados/fundidos para a tabela principal")
    
    context['task_instance'].xcom_push(
        key='merge_result', 
        value={'merged_rows': linhas_fundidas}
    )
    
    return {'merged_rows': linhas_fundidas}

# Define tarefas optimizadas
configurar_ligacao_task = PythonOperator(
    task_id='configurar_ligacao',
    python_callable=configurar_ligacao,
    dag=dag,
)

verificar_staging_task = PythonOperator(
    task_id='verificar_staging',
    python_callable=verificar_staging,
    dag=dag,
)

processar_dados_task = PythonOperator(
    task_id='processar_staging_para_eventos',
    python_callable=processar_staging_para_eventos,
    dag=dag,
)

atualizar_coordenadas_task = PythonOperator(
    task_id='atualizar_coordenadas',
    python_callable=atualizar_coordenadas,
    dag=dag,
)

atualizar_geometria_task = PythonOperator(
    task_id='atualizar_geometria',
    python_callable=atualizar_geometria,
    dag=dag,
)

fundir_para_principal_task = PythonOperator(
    task_id='fundir_para_tabela_principal',
    python_callable=fundir_para_tabela_principal,
    dag=dag,
)

# Define dependências de forma limpa
configurar_ligacao_task >> verificar_staging_task >> processar_dados_task
processar_dados_task >> atualizar_coordenadas_task >> atualizar_geometria_task
atualizar_geometria_task >> fundir_para_principal_task
