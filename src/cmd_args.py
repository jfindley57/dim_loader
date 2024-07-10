import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="usage %prog [options]")

    parser.add_argument("-c", "--config",
                        action="store",
                        default='dimension_config.ini',
                        dest="config_file",
                        help="Which config_file to use")

    parser.add_argument("-t", "--tables",
                        action="store",
                        dest="tables",
                        help="Dump Dimension for Table")

    parser.add_argument("--full",
                        action="store_true",
                        dest="full_table",
                        help="Dump entire table")

    parser.add_argument("-v", "--view",
                        action="store_true",
                        dest="use_views",
                        help="Use Views (append vw_)")

    parser.add_argument("--mysql",
                        action="store_true",
                        dest="mysql_validation",
                        help="Compare against mysql (dimension_validation.py)")

    parser.add_argument("-d", "--date",
                        action="store",
                        dest="custom_date",
                        help="Upload to Custom Date")

    parser.add_argument("--rerun",
                        action="store_true",
                        dest="rerun_dimension",
                        help="Checks for failed run_times and reruns them for gdm")

    return vars(parser.parse_args())


arg_opt = parse_args()