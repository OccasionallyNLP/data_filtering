for DATA in 2021-17 2021-21 2021-25 2021-31 2021-39 2021-43 2021-49 2022-05 2022-21 2022-27 2022-33 2022-40 2023-06 2023-14 2023-23 2023-40 2023-50
do
    python run_datatrove_mp.py --output_dir ../kt_output/heuristic_filtering/test --data_path /home/work/user/wjpark/midm_pack/data/$DATA/remove_duplicate
done
