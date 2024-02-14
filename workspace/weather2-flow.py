import httpx
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.artifacts import create_markdown_artifact
from prefect.blocks.system import JSON
from datetime import timedelta

@task(retries=4, retry_delay_seconds=0.5, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))  # or [0.1, 0.5, 1, 2])
def fetch_lat_long():
    random_code = httpx.get("https://httpstat.us/Random/200,500", verify=False)
    if random_code.status_code >= 400:
        raise Exception()
    print(random_code.text)
    json_block = JSON.load("lat-long")
    return json_block.value


@task
def fetch_weather(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    temps = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )
    forecasted_temp = float(temps.json()["hourly"]["temperature_2m"][0])
    print(f"Forecasted temp C: {forecasted_temp} degrees")
    return forecasted_temp


@task
def save_weather(temp: float):
    with open("weather.csv", "w+") as w:
        w.write(str(temp))
    return "Successfully wrote temp"


@task
def report(temp):
    markdown_report = f"""# Weather Report
    
## Recent weather

| Time        | Temperature |
|:--------------|-------:|
| Temp Forecast  | {temp} |
"""
    create_markdown_artifact(
        key="weather-report",
        markdown=markdown_report,
        description="Very scientific weather report",
    )

@flow()
def pipeline():
    lat_long = fetch_lat_long()
    temp = fetch_weather(lat_long['lat'], lat_long['long'])
    result = save_weather(temp)
    report(temp)
    return result


if __name__ == "__main__":
    pipeline()