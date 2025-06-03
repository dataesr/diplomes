import os

from application.server.main.logger import get_logger
from diplomes.u3_apply_cleaning_functions import corrige
from diplomes.u1_google_sheets import get_all_correctifs as u1
from diplomes.u4_generate_od_file import generate_od

logger = get_logger(__name__)

DATA_PATH = os.getenv("MOUNTED_VOLUME_INSCRITS")


def create_task_corrige(args):
    logger.debug('Test')
    try:
        chx = ""
        if args.get('google', True):
            print("google")
            chx = "google"
        elif args.get('json', False):
            print("json")
            chx = "json"

        if chx in ["google", "json"]:
            cor_dict = u1(chx)

        if "cor_dict" in locals():
            if args.get('corrige', True):
                # si google_sheets modifiées peut-être demander en paramètre si on recharge ou pas excel ?
                # chargement_google_sheets= args.get('chargement_google_sheets')
                corrige(cor_dict)
                generate_od()
                print("Ca marche", flush=True)
                logger.debug(f'Done!')
        else:
            raise ValueError("La variable cor_dict n'existe pas")
    except ValueError:
        print("Il faut utiliser soit \"google\", soit \"json\" pour pouvoir effectuer la correction")

