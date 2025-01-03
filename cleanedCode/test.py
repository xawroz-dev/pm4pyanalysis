# test_pipeline.py

def main():
    from global_storage import GlobalStorage
    from process_mining import ProcessMiningService

    storage = GlobalStorage()
    service = ProcessMiningService(storage)

    # 1) Import log + generate variants
    file_path = "output.xes"
    log_id = "test_log"
    print(service.generate_variants_from_xes(file_path, log_id))

    # See the named variants
    variant_dict = storage.get_named_variants(log_id)
    print(f"Named variants for '{log_id}':", list(variant_dict.keys()))


    # # 2) Add a custom variant
    # service.add_custom_variant(log_id, "Variant_Custom", ["A","B","C","D"])
    # variant_dict = storage.get_variants(log_id)
    # print("After adding custom variant, keys:", list(variant_dict.keys()))

    # 3) Discover a model from some subset, e.g. ["Variant_1", "Variant_Custom"]
    chosen_variants = ["Variant_1", "Variant_2"]
    model_id = "my_model"
    model_data = service.discover_process_from_variants(log_id, chosen_variants, model_id)
    print(model_data)
    print("Discovered model data metrics:", model_data["metrics"])

    # 4) Check fitness on just ["Variant_Custom"]
    fit_result = service.check_fitness_only(model_id, log_id, ["Variant_1"])
    print("Fitness result:", fit_result)

    # 5) Create a snapshot
    snapshot_id = "snap_test_001"
    snapshot_data = service.create_snapshot(snapshot_id, model_id, log_id, chosen_variants)
    print("Snapshot data keys:", snapshot_data.keys())
    print("Snapshot content:\n", snapshot_data)


if __name__ == "__main__":
    main()
