import prefect 

@prefect.task
def post_plans(wait_time_days):
    print(f"waiting {wait_time_days} days before destroying earth")
    return wait_time_days
@prefect.task
def destroy_planet(days_waited, planet_name):
    print(f"{planet_name} is now gone!")
    return f"location_of_planet_{planet_name}"
@prefect.task
def pave_highway(location):
    print(location)
with prefect.Flow('Intergalactic Highway') as flow:
    wait_time_days = prefect.Parameter("wait_time_days")
    planet_name = prefect.Parameter("planet_name")
    days_waited = post_plans(wait_time_days=wait_time_days)
    location = destroy_planet(days_waited, planet_name=planet_name)
    pave_highway(location)

flow.deploy("test", 
            prefect_version="0.7.0",
            # registry_url="gcr.io/prefect-staging-5cd57f/alex-test-flows",
            local_image=True,
            )