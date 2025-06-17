import random
from datetime import datetime, timedelta
from faker import Faker

# Initialize Faker
fake = Faker()

class ProcessMiningDataGenerator:
    def __init__(self, locale='en_US'):
        self.fake = Faker(locale)

    def _evaluate_faker_string(self, value):
        """
        Evaluates a string that looks like a Faker method call.
        Example: "fake.name()" -> generates a fake name.
        """
        if isinstance(value, str) and value.startswith("fake.") and value.endswith("()"):
            try:
                # Dynamically call the faker method
                method_name = value[5:-2] # remove "fake." and "()"
                return getattr(self.fake, method_name)()
            except AttributeError:
                print(f"Warning: Faker method '{method_name}' not found. Returning original string.")
                return value
        return value

    def _process_data_recursive(self, data, fill_with_faker):
        """
        Recursively processes dictionaries and lists to fill Faker methods.
        """
        if isinstance(data, dict):
            processed_data = {}
            for key, value in data.items():
                if fill_with_faker:
                    processed_data[key] = self._process_data_recursive(value, fill_with_faker)
                else:
                    processed_data[key] = value # Return as is if flag is False
            return processed_data
        elif isinstance(data, list):
            processed_data = []
            for item in data:
                if fill_with_faker:
                    processed_data.append(self._process_data_recursive(item, fill_with_faker))
                else:
                    processed_data.append(item) # Return as is if flag is False
            return processed_data
        else:
            if fill_with_faker:
                return self._evaluate_faker_string(data)
            else:
                return data # Return as is if flag is False

    def generate_data(self, data_template: dict, fill_with_faker: bool) -> dict:
        """
        Generates data based on a given template.
        If fill_with_faker is True, it evaluates Faker method calls in the template.

        Args:
            data_template (dict): A dictionary representing the desired data format.
                                  Values can be strings like "fake.name()" or "fake.address()".
            fill_with_faker (bool): If True, Faker methods are evaluated.
                                    If False, the template is returned as is without evaluation.

        Returns:
            dict: The generated data, with Faker methods replaced if fill_with_faker is True.
        """
        if not isinstance(data_template, dict):
            raise ValueError("data_template must be a dictionary.")

        return self._process_data_recursive(data_template, fill_with_faker)

    def generate_journey_data(self, num_journeys: int = 1, start_date: datetime = None, max_duration_hours: int = 72) -> list[dict]:
        """
        Generates fake credit card issuance journey data with variations.

        Args:
            num_journeys (int): The number of unique credit card issuance journeys to generate.
            start_date (datetime, optional): The starting datetime for the first activity.
                                             Defaults to now if None.
            max_duration_hours (int): Maximum duration for a single journey in hours.

        Returns:
            list[dict]: A list of journey events, each event being a dictionary.
                        Each dictionary represents an activity in a specific case.
        """
        if start_date is None:
            start_date = datetime.now()

        # Define process activities with their typical durations
        activities = {
            "Application Submitted": (10, 30), # minutes
            "Application Received": (5, 15),
            "Identity Verification": (60, 180), # minutes
            "Credit Check": (30, 120),
            "Fraud Check": (30, 120),
            "Decision Pending": (120, 300), # minutes
            "Documents Requested": (120, 480), # longer wait for customer action
            "Documents Received": (60, 240),
            "Additional Review Required": (180, 480), # longer
            "Manual Review": (240, 720), # even longer
            "Application Approved": (15, 60),
            "Card Production": (240, 720), # hours, but represented as minutes here
            "Card Shipped": (480, 1440), # longer for shipping
            "Card Activated": (10, 60),
            "Application Rejected": (15, 60),
            "Customer Notified of Rejection": (30, 120)
        }

        all_journey_events = []

        for i in range(num_journeys):
            case_id = f"CC_APP_{self.fake.unique.random_int(min=100000, max=999999)}"
            current_time = start_date + timedelta(minutes=random.randint(0, 60*24*7)) # Spread journeys over a week
            journey_path = []
            status = "pending" # Track if approved or rejected

            # Common initial activities
            path_segment_1 = [
                "Application Submitted",
                "Application Received",
                "Identity Verification"
            ]
            for activity in path_segment_1:
                duration_min = random.randint(*activities[activity])
                journey_path.append({
                    "case_id": case_id,
                    "activity": activity,
                    "timestamp": current_time.isoformat(),
                    "resource": self.fake.random_element(elements=('System', 'Online Portal', 'Applicant')),
                    "application_id": self.fake.uuid4(),
                    "customer_id": self.fake.uuid4(),
                    "application_source": self.fake.random_element(elements=('Online', 'Branch', 'Referral')),
                    "country": self.fake.country_code(),
                })
                current_time += timedelta(minutes=duration_min)
                if current_time - start_date > timedelta(hours=max_duration_hours):
                    status = "timed_out"
                    break # Break if journey is too long

            if status == "timed_out":
                continue # Skip to next journey if this one timed out early


            # Branching after Identity Verification: Documents Request (optional loop)
            documents_requested = False
            if random.random() < 0.2: # 20% chance to request documents
                documents_requested = True
                num_document_requests = random.choice([1, 1, 1, 2]) # Mostly one request, sometimes two
                for _ in range(num_document_requests):
                    if status == "timed_out": break
                    duration_req = random.randint(*activities["Documents Requested"])
                    journey_path.append({
                        "case_id": case_id,
                        "activity": "Documents Requested",
                        "timestamp": current_time.isoformat(),
                        "resource": self.fake.random_element(elements=('Compliance Team', 'System')),
                        "document_type": self.fake.random_element(elements=('Proof of Address', 'Proof of Income', 'ID Copy')),
                        "request_reason": self.fake.sentence(nb_words=5),
                    })
                    current_time += timedelta(minutes=duration_req)
                    if current_time - start_date > timedelta(hours=max_duration_hours):
                        status = "timed_out"
                        break

                    if status == "timed_out": break
                    duration_rec = random.randint(*activities["Documents Received"])
                    journey_path.append({
                        "case_id": case_id,
                        "activity": "Documents Received",
                        "timestamp": current_time.isoformat(),
                        "resource": self.fake.random_element(elements=('Applicant', 'System')),
                        "document_status": self.fake.random_element(elements=('Complete', 'Incomplete')),
                    })
                    current_time += timedelta(minutes=duration_rec)
                    if current_time - start_date > timedelta(hours=max_duration_hours):
                        status = "timed_out"
                        break
                    # If documents incomplete, another request might happen, handled by loop

            if status == "timed_out":
                continue # Skip to next journey if this one timed out early


            # Credit Check & Fraud Check (order can vary)
            if random.random() < 0.5: # 50% chance for Credit Check then Fraud Check
                core_checks = ["Credit Check", "Fraud Check"]
            else:
                core_checks = ["Fraud Check", "Credit Check"]

            for activity in core_checks:
                if status == "timed_out": break
                duration = random.randint(*activities[activity])
                journey_path.append({
                    "case_id": case_id,
                    "activity": activity,
                    "timestamp": current_time.isoformat(),
                    "resource": self.fake.random_element(elements=('System', 'Credit Bureau', 'Fraud Detection System')),
                    "check_result": self.fake.random_element(elements=('Pass', 'Fail', 'Refer')),
                })
                current_time += timedelta(minutes=duration)
                if current_time - start_date > timedelta(hours=max_duration_hours):
                    status = "timed_out"
                    break

            if status == "timed_out":
                continue # Skip to next journey if this one timed out early

            # Decision Point
            if random.random() < 0.15: # 15% chance of rejection
                status = "rejected"
            elif random.random() < 0.2: # 20% chance of additional review
                status = "additional_review"
            else:
                status = "approved"

            duration_pending = random.randint(*activities["Decision Pending"])
            journey_path.append({
                "case_id": case_id,
                "activity": "Decision Pending",
                "timestamp": current_time.isoformat(),
                "resource": self.fake.random_element(elements=('System', 'Underwriting Team')),
                "decision_criteria_met": status != "rejected", # Simplified
            })
            current_time += timedelta(minutes=duration_pending)
            if current_time - start_date > timedelta(hours=max_duration_hours):
                status = "timed_out"


            if status == "timed_out":
                continue # Skip to next journey if this one timed out early

            if status == "additional_review":
                duration_ar = random.randint(*activities["Additional Review Required"])
                journey_path.append({
                    "case_id": case_id,
                    "activity": "Additional Review Required",
                    "timestamp": current_time.isoformat(),
                    "resource": self.fake.random_element(elements=('Senior Underwriter', 'Compliance Officer')),
                    "review_reason": self.fake.sentence(nb_words=7),
                })
                current_time += timedelta(minutes=duration_ar)
                if current_time - start_date > timedelta(hours=max_duration_hours):
                    status = "timed_out"
                if status != "timed_out":
                    duration_mr = random.randint(*activities["Manual Review"])
                    journey_path.append({
                        "case_id": case_id,
                        "activity": "Manual Review",
                        "timestamp": current_time.isoformat(),
                        "resource": self.fake.random_element(elements=('Specialist Team', 'Fraud Analyst')),
                        "review_outcome": self.fake.random_element(elements=('Approved', 'Rejected', 'More Info Needed')),
                    })
                    current_time += timedelta(minutes=duration_mr)
                    if current_time - start_date > timedelta(hours=max_duration_hours):
                        status = "timed_out"

                # After additional review, it can still be approved or rejected
                if status != "timed_out":
                    status = random.choice(["approved", "rejected"]) # Final decision after review

            if status == "timed_out":
                continue # Skip to next journey if this one timed out early


            if status == "approved":
                approved_path = [
                    "Application Approved",
                    "Card Production",
                    "Card Shipped",
                    "Card Activated"
                ]
                for activity in approved_path:
                    if status == "timed_out": break
                    duration = random.randint(*activities[activity])
                    event_data = {
                        "case_id": case_id,
                        "activity": activity,
                        "timestamp": current_time.isoformat(),
                        "resource": self.fake.random_element(elements=('System', 'Card Manufacturing', 'Logistics')),
                        "card_type": self.fake.random_element(elements=('Visa Gold', 'MasterCard Platinum', 'Amex Green')),
                    }
                    if activity == "Card Shipped":
                        event_data["tracking_number"] = self.fake.bothify(text='????##########')
                        event_data["shipping_carrier"] = self.fake.random_element(elements=('FedEx', 'UPS', 'DHL'))
                    if activity == "Card Activated":
                        event_data["activation_method"] = self.fake.random_element(elements=('Online', 'Phone', 'Mobile App'))
                    journey_path.append(event_data)
                    current_time += timedelta(minutes=duration)
                    if current_time - start_date > timedelta(hours=max_duration_hours):
                        status = "timed_out"
                        break

            elif status == "rejected":
                rejected_path = [
                    "Application Rejected",
                    "Customer Notified of Rejection"
                ]
                for activity in rejected_path:
                    if status == "timed_out": break
                    duration = random.randint(*activities[activity])
                    journey_path.append({
                        "case_id": case_id,
                        "activity": activity,
                        "timestamp": current_time.isoformat(),
                        "resource": self.fake.random_element(elements=('Underwriting Team', 'System')),
                        "rejection_reason": self.fake.random_element(elements=(
                            'Low Credit Score', 'Incomplete Documents', 'Fraud Detected',
                            'High Debt-to-Income Ratio', 'Insufficient Income'
                        )),
                    })
                    current_time += timedelta(minutes=duration)
                    if current_time - start_date > timedelta(hours=max_duration_hours):
                        status = "timed_out"
                        break

            # Add the completed journey path to the overall list
            if status != "timed_out":
                all_journey_events.extend(journey_path)

        return all_journey_events

# Example Usage:
if __name__ == "__main__":
    generator = ProcessMiningDataGenerator()

    print("--- API 1: generate_data Examples ---")
    # Example 1: Fill with Faker methods (fill_with_faker=True)
    template_with_faker_methods = {
        "user_id": "fake.uuid4()",
        "name": "fake.name()",
        "email": "fake.email()",
        "address": {
            "street": "fake.street_address()",
            "city": "fake.city()",
            "zip_code": "fake.postcode()",
            "country": "fake.country()"
        },
        "registration_date": "fake.date_this_year().isoformat()",
        "status": "active",
        "items_purchased": [
            {"item_id": "fake.unique.random_number(digits=5)", "quantity": 1},
            {"item_id": "fake.unique.random_number(digits=5)", "quantity": 2}
        ]
    }
    generated_data_with_faker = generator.generate_data(template_with_faker_methods, True)
    print("\nGenerated Data (fill_with_faker=True):\n", generated_data_with_faker)

    # Example 2: Return template as is (fill_with_faker=False)
    generated_data_without_faker = generator.generate_data(template_with_faker_methods, False)
    print("\nGenerated Data (fill_with_faker=False - template returned as is):\n", generated_data_without_faker)

    # Example 3: Simple template
    simple_template = {
        "event_name": "Login Attempt",
        "timestamp": "fake.date_time_this_month().isoformat()"
    }
    generated_simple_data = generator.generate_data(simple_template, True)
    print("\nGenerated Simple Data (fill_with_faker=True):\n", generated_simple_data)


    print("\n--- API 2: generate_journey_data Example ---")
    # Generate 5 credit card issuance journeys
    journey_events = generator.generate_journey_data(num_journeys=5, start_date=datetime(2023, 1, 1))

    # Print first 20 events to show structure (can be many events if num_journeys is high)
    print(f"\nGenerated {len(journey_events)} journey events across 5 cases. First 20 events:")
    for event in journey_events[:20]:
        print(event)

    # You can then use this 'journey_events' list for process mining tools.
    # For example, to save to a CSV for ProM or other tools:
    # import pandas as pd
    # df = pd.DataFrame(journey_events)
    # df.to_csv("credit_card_journeys.csv", index=False)
    # print("\nData saved to credit_card_journeys.csv")
