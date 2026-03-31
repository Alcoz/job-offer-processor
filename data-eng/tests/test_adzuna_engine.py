from dotenv import load_dotenv
import os
from data_eng.adzuna_engine import adzuna_job_offers_getter


class TestAdzunaApiErrors:
    @classmethod
    def setup_class(cls):
        load_dotenv()
        cls.adzuna_api_id = os.getenv("ADZUNA_API_ID")
        cls.adzuna_api_key = os.getenv("ADZUNA_API_KEY")

    def test_adzuna_api_data_non_empty_list(self):
        job_list_test = adzuna_job_offers_getter(
            self.adzuna_api_id, self.adzuna_api_key
        )

        assert len(job_list_test) > 0

    def test_adzuna_api_data_shape(self):
        job_data_test = adzuna_job_offers_getter(
            self.adzuna_api_id, self.adzuna_api_key
        )[0]

        print(job_data_test)

        key_list_attented = [
            "adref",
            "salary_is_predicted",
            "latitude",
            "longitude",
            "location",
            "description",
            "company",
            "title",
        ]

        assert all(key in job_data_test.keys() for key in key_list_attented)
