for DATA in 2019-13 2019-18 2019-22 2019-26 2019-30 2019-35 2019-39 2019-43 2019-47 2019-51 2020-05 2020-10 2020-16 2020-24 2020-29 2020-34 2020-40 2020-45 2020-50 2021-04 2021-10 

do
    python run_datatrove_mp.py --output_dir ../kt_output/heuristic_filtering/test --data_path /home/work/user/wjpark/midm_pack/data/$DATA/remove_duplicate
done
