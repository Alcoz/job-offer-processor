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


def adzuna_job_offers_cleaner(adzuna_job_offers):
    adzuna_job_list_cleaned = []

    for job_offer in adzuna_job_offers:
        job_offer_clean = {
            "id": job_offer["id"],
            "company": job_offer["company"]["display_name"],
            "title": job_offer["title"],
            "description": job_offer["description"],
            "latitude": job_offer.get("latitude", ""),
            "longitude": job_offer.get("longitude", ""),
            "location": job_offer["location"]["display_name"],
        }

        if job_offer["salary_is_predicted"] == "1":
            job_offer_clean["salary_min"] = job_offer["salary_min"]
            job_offer_clean["salary_max"] = job_offer["salary_max"]

        adzuna_job_list_cleaned.append(job_offer_clean)

    return adzuna_job_list_cleaned
