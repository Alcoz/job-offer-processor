import requests

ADZUNA_API_URL = "http://api.adzuna.com/v1/api/jobs/gb/search/1?app_id={adzuna_api_id}&app_key={adzuna_api_key}&results_per_page=20&what=data%20engineer&content-type=application/json"


def adzuna_job_offers_getter(adzuna_api_id, adzuna_api_key):
    api_call = ADZUNA_API_URL.format(
        adzuna_api_id=adzuna_api_id, adzuna_api_key=adzuna_api_key
    )

    adzuna_job_offers_api_response = requests.get(api_call)

    assert adzuna_job_offers_api_response.status_code == 200

    adzuna_job_offers = adzuna_job_offers_api_response.json()["results"]

    return adzuna_job_offers
