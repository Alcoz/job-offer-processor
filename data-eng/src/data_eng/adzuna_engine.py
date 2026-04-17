import requests

ADZUNA_API_URL = "http://api.adzuna.com/v1/api/jobs/fr/search/1?app_id={adzuna_api_id}&app_key={adzuna_api_key}&results_per_page=1000&what=data%20engineer&content-type=application/json"


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
        job_offer["company"] = job_offer["company"]["display_name"]
        job_offer["location"] = (job_offer["location"]["display_name"],)

        job_offer_clean = {
            "title": job_offer.get("title", ""),
            "created": job_offer.get("created"),
            "company": job_offer.get("company"),
            "location": job_offer.get("location"),
            "longitude": job_offer.get("longitude"),
            "latitude": job_offer.get("latitude"),
            "salary_min": job_offer.get("salary_min"),
            "salary_max": job_offer.get("salary_max"),
            "redirect_url": job_offer.get("redirect_url"),
        }

        adzuna_job_list_cleaned.append(job_offer_clean)

    return adzuna_job_list_cleaned
