import random

def validate_stitching(jm, generated_data, sample_size=20):
    print("\n[Validation] Validating random sample of journeys...")
    
    samples = random.sample(list(generated_data.keys()), min(sample_size, len(generated_data)))
    success_count = 0
    
    for j_idx in samples:
        expected_events = set(generated_data[j_idx])
        probe_event = list(expected_events)[0]
        
        journey_info = jm.get_journey(probe_event)
        
        if not journey_info:
            print(f"  [FAIL] Journey not found for event {probe_event}")
            continue
            
        actual_events = set(journey_info['events'])
        
        if expected_events.issubset(actual_events):
            if len(actual_events) == len(expected_events):
                success_count += 1
            else:
                print(f"  [WARN] Journey {journey_info['journey_id']} has {len(actual_events)} events, expected {len(expected_events)}.")
        else:
            missing = expected_events - actual_events
            print(f"  [FAIL] Journey {journey_info['journey_id']} missing events: {missing}")

    print(f"[Validation] Result: {success_count}/{len(samples)} passed.")
    return success_count == len(samples)
