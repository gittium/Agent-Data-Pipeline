# type: ignore
from datetime import datetime
from airflow.decorators import task, dag
import psycopg2

DATABASE_CONFIG = {
    "host": "postgres-datawarehouse",
    "database": "taxi_datawarehouse",
    "user": "datawarehouse_user",
    "password": "datawarehouse_password",
    "port": 5432,
}

def get_pg_conn():
    return psycopg2.connect(**DATABASE_CONFIG)

@dag(
    dag_id="taxi_transform_dag_v2",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["transform", "analytics"],
)
def taxi_transform_dag():

    @task
    def validate_raw():
        conn = get_pg_conn(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM raw.taxi_trips_raw")
        count = cur.fetchone()[0]
        if count == 0:
            raise ValueError("No data in raw.taxi_trips_raw")
        print(f"✅ Found {count} raw records.")
        cur.close(); conn.close()
        return count

    @task
    def transform_data(record_count: int):
        conn = get_pg_conn(); cur = conn.cursor()
        print("🚀 Cleaning and transforming raw data...")
        cur.execute("TRUNCATE TABLE analytics.taxi_trips_cleaned;")
        transform_sql = """
        INSERT INTO analytics.taxi_trips_cleaned (
            pickup_datetime, dropoff_datetime, trip_duration_minutes,
            pickup_hour, pickup_day_of_week, pickup_month,
            trip_distance, fare_amount, tip_amount, tip_percentage,
            total_amount, payment_method, passenger_count,
            revenue_per_mile, trip_category, congestion_fee,
            pickup_location_id, dropoff_location_id
        )
        SELECT
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60,
            EXTRACT(hour FROM tpep_pickup_datetime),
            EXTRACT(dow FROM tpep_pickup_datetime),
            EXTRACT(month FROM tpep_pickup_datetime),
            trip_distance, fare_amount, tip_amount,
            CASE WHEN fare_amount>0 THEN LEAST((tip_amount/fare_amount)*100,999.99) ELSE 0 END,
            total_amount,
            CASE payment_type
                WHEN 1 THEN 'Credit Card' WHEN 2 THEN 'Cash'
                WHEN 3 THEN 'No Charge' WHEN 4 THEN 'Dispute'
                ELSE 'Other' END,
            passenger_count,
            CASE WHEN trip_distance>0 THEN total_amount/trip_distance ELSE 0 END,
            CASE
                WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60<5 THEN 'Very Short'
                WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60<15 THEN 'Short'
                WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60<30 THEN 'Medium'
                WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60<60 THEN 'Long'
                ELSE 'Very Long' END,
            COALESCE(cbd_congestion_fee,0),
            PULocationID, DOLocationID
        FROM raw.taxi_trips_raw
        WHERE tpep_dropoff_datetime>tpep_pickup_datetime
          AND trip_distance>0
          AND total_amount>=0;
        """
        cur.execute(transform_sql)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM analytics.taxi_trips_cleaned")
        result = cur.fetchone()[0]
        print(f"✅ Transformed {result} rows.")
        cur.close(); conn.close()
        return result

    @task
    def generate_summary(transformed_rows: int):
        conn = get_pg_conn(); cur = conn.cursor()
        print("📊 Generating analytics summary...")
        cur.execute("""
            SELECT COUNT(*), AVG(trip_distance), AVG(total_amount), AVG(tip_percentage)
            FROM analytics.taxi_trips_cleaned
        """)
        stats = cur.fetchone()
        print(f"Total trips: {stats[0]}, Avg distance: {stats[1]:.2f}, "
              f"Avg fare: {stats[2]:.2f}, Avg tip %: {stats[3]:.2f}")
        cur.close(); conn.close()
        return stats

    raw_check = validate_raw()
    transform = transform_data(raw_check)
    summary = generate_summary(transform)
    raw_check >> transform >> summary

dag_instance = taxi_transform_dag()
