from confluent_kafka import Consumer, KafkaError
import certifi
import json
import pandas as pd

def extraer_datos_kafka(password_profesor):
    conf = {
        'bootstrap.servers': 'kafka-classroom-2026.westus.cloudapp.azure.com:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'SCRAM-SHA-512',
        'sasl.username': 'classroom-students',
        'sasl.password': password_profesor,
        'ssl.ca.location': certifi.where(),
        'group.id': 'alexander-mosquera-fajardo',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe(['etl.records'])
    
    registros = []
    print("Conectando a Kafka y extrayendo registros...")
    
    try:
        intentos_vacios = 0
        while True:
            msg = consumer.poll(timeout=2.0)
            
            if msg is None:
                intentos_vacios += 1
                print(f"Esperando datos de Kafka... (Intento {intentos_vacios}/5)")
                if intentos_vacios >= 5:
                    print("No llegaron más mensajes. Terminando lectura.")
                    break
                continue
            
            # Reseteamos el contador si llega un mensaje
            intentos_vacios = 0
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Error de consumo: {msg.error()}")
                    break
            
            datos = json.loads(msg.value().decode('utf-8'))
            registros.append(datos)
            
    finally:
        consumer.close()
        
    print(f"Extracción finalizada. Total de registros consumidos: {len(registros)}")
    
    df = pd.DataFrame(registros)
    return df

if __name__ == '__main__':
    PASSWORD_KAFKA = "06058462dc7f13b89eb5ace927b989984e77536417605379" 
    
    df_felicidad = extraer_datos_kafka(PASSWORD_KAFKA)
    
    if not df_felicidad.empty:
        print("\nMuestra de los datos extraídos:")
        print(df_felicidad.head())
        
        df_felicidad.to_csv("datos_crudos_kafka.csv", index=False)
        print("\nDatos guardados localmente en 'datos_crudos_kafka.csv' para la fase de EDA.")
