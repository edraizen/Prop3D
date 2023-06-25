import os
import re
import sys
import urllib.request
import multiprocessing
from numbers import Number
from collections import defaultdict

import pandas as pd
from joblib import Parallel, delayed
from toil.realtimeLogger import RealtimeLogger

from Prop3D.util.toil import map_job
from Prop3D.generate_data.create_data_splits import split_dataset_at_level
from Prop3D.generate_data.update_pdb import get_all_pdbs, get_custom_pdbs

try:
    import h5pyd as h5py
    DISTRIBUTED = True
except ImportError:
    try:
        import h5py
        DISTRIBUTED = False
    except:
        raise ImportError("h5pyd or h5py must be installed")

def split_superfamily_at_level(job, cath_full_h5, superfamily, sfam_df, level_key, level_name,
  split_size={"train":0.8, "validation":0.1, "test":0.1}):
    """Split a dataset into train/validation/test sets, saving the splits into new h5 groups with
    links back to the the main dataset.
    
    Paramters:
    ----------
    job : toi.job.Job
        Toil job
    cath_full_h5 : str
        Path to H5 file on HSDS enpoint
    superfamily : str
        Group prefix, can be empty ('') for h5 file
    sfam_df : pd.DataFrame
        The data frame to split. Each row must a single protein and the df M\must contain 2 columns: 
            (i) "cath_domain", the column of protein domain names, must match groups of the same name in this 'superfamily' group;
            (ii) level_key, custom variable name for the name of the cluster the protein domain belongs to
    level_name : str
        Name of the column that contains cluster names
    split_size : Dict [split_name->split perecent]
        A dictionary containing the total number of splits
    """
    return split_dataset_at_level(job, cath_full_h5, superfamily, sfam_df, level_key, level_name,
  split_size={"train":0.8, "validation":0.1, "test":0.1})


def create_splits_for_superfamily_levels(job, sfam, cath_full_h5):
    superfamily, sfam_df = sfam
    RealtimeLogger.info(f"Start splits for {superfamily}")

    with h5py.File(cath_full_h5, mode="a", use_cache=False, retries=100) as store:
        store.require_group(f"{superfamily}/data_splits")

    for level_key, level_name in [("S", "S35"), (list("SO"), "S60"), (list("SOL"), "S95"), (list("SOLI"), "S100")]:
        job.addChildJobFn(split_superfamily_at_level, cath_full_h5, superfamily, sfam_df, level_key, level_name)

def create_representatives_for_superfamily(job, sfam, cath_full_h5):
    from Prop3D.parsers.cath import CATHApi
    cath = CATHApi(job=job)
    hierarchy = cath.list_children_in_heirarchy(sfam, 5)
    representatives = [child["example_domain_id"] for child in hierarchy["children"]]

    key = f"{sfam.replace('.', '/')}/representatives"

    missing_domains = []

    with h5py.File(cath_full_h5, mode="a", use_cache=False, retries=100) as store:
        group = store.require_group(key)

        for domain in representatives:
            try:
                group[domain] = store[f"{sfam.replace('.', '/')}/domains/{domain}'"]
            except KeyError:
                missing_domains.append(domain)

        if len(missing_domains) > 0:
            store[key].attrs["missing_domains"] = missing_domains
        store[key].attrs["total_domains"] = len(representatives)

def create_splits(job, cath_full_h5, all_superfamilies):
    RealtimeLogger.info(f"Start all splits {all_superfamilies}")
    sfams = [g for g in all_superfamilies.groupby("h5_key")]
    map_job(job, create_splits_for_superfamily_levels, sfams, cath_full_h5)
    map_job(job, create_representatives_for_superfamily, [s.iloc[0].cath_code for _, s in sfams], cath_full_h5)
    job.addFollowOnJobFn(finish_section, cath_full_h5, "completed_domain_splits")
    job.addFollowOnJobFn(finish_section, cath_full_h5, "completed_representatives")

def process_cath_domain_list_for_group(job, group, cath_full_h5):
    name, group_df = group

    with h5py.File(cath_full_h5, mode="a", use_cache=False, retries=100) as store:
        for _, row in group_df.iterrows():
            group = store.require_group(f"{row.h5_key}/domains/{row.cath_domain}")
            group.domain_length = row.domain_length
            group.resolution = row.resolution

def process_cath_domain_list(job, cath_full_h5, cathcode=None, skip_cathcode=None, force=False, work_dir=None):
    if work_dir is None:
        if job is not None and hasattr(job, "fileStore"):
            work_dir = job.fileStore.getLocalTempDir()
        else:
            work_dir = os.getcwd()

    run_domain_names = True
    run_splits = True

    if isinstance(force, bool) or (is_num(force) and int(force)<3):
        try:
            with h5py.File(cath_full_h5, mode="r", use_cache=False, retries=100) as store:
                run_domain_names = not store.attrs.get("completed_domain_list", False)
                run_splits = not store.attrs.get("completed_domain_splits", False)
        except IOError:
            raise

    if not run_splits:
        #Already exists do not run again
        return

    #Run splits needs same info, so build pandas frame for both:

    cath_domain_list_file = os.path.join(work_dir, "cath-domain-list.txt")
    if not os.path.isfile(cath_domain_list_file):
        urllib.request.urlretrieve(
            "ftp://orengoftp.biochem.ucl.ac.uk/cath/releases/latest-release/cath-classification-data/cath-domain-list.txt",
            cath_domain_list_file)

    names = pd.read_csv(cath_domain_list_file, delim_whitespace=True, header=None, comment="#",
        names=["cath_domain", *list("CATHSOLID"), "domain_length", "resolution"])
    names = names.assign(cath_code=names["C"].astype(str)+"."+names["A"].astype(str)+"."+names["T"].astype(str)+"."+names["H"].astype(str))
    names = names.assign(h5_key="/"+names["cath_code"].str.replace(".","/"))
    names = names.assign(group=names["cath_code"].str.split(".", expand=True)[[0,1]].fillna("").agg('.'.join, axis=1))

    if cathcode is not None:
        if not isinstance(cathcode, (list, tuple)):
            cathcode = [cathcode]

        use_sfams = tuple([".".join(sfam)+"." if isinstance(sfam, (list,tuple)) else sfam.replace("/", ".")+"." \
            for sfam in cathcode])

        names = names[(names["cath_code"]+".").str.startswith(use_sfams)]

    if skip_cathcode is not None:
        if not isinstance(skip_cathcode, (list, tuple)):
            skip_cathcode = [skip_cathcode]

        skip_sfams = tuple([".".join(sfam)+"." if isinstance(sfam, (list,tuple)) else sfam.replace("/", ".")+"." \
            for sfam in skip_cathcode])

        names = names[~(names["cath_code"]+".").str.startswith(skip_sfams)]

    if run_domain_names:
        groups = [g for g in names.groupby("cath_code")]
        map_job(job, process_cath_domain_list_for_group, groups, cath_full_h5)

        job.addFollowOnJobFn(finish_section, cath_full_h5, "completed_domain_list")

    #Create splits
    all_superfamilies = names[names['cath_code'].str.split('.').agg(len)==4]
    job.addFollowOnJobFn(create_splits, cath_full_h5, all_superfamilies)

def process_cath_names_for_group(job, group, cath_full_h5, level=2):
    name, group_df = group
    RealtimeLogger.info(f"Adding group under {name}")

    group_df = group_df[group_df.cath_code.str.count(".")==level-1]

    with h5py.File(cath_full_h5, mode="a", use_cache=False, retries=100) as store:
        for _, row in group_df.iterrows():
            group = store.require_group(row.h5_key)
            try:
                group.description = row.description
                group.representativeDomain = row.representative
            except KeyError:
                #likely from direcotry
                group.description.description = None
                group.representativeDomain = None

            if row.cath_code.count(".") == 3:
                store.require_group(f"{row.h5_key}/domains")
    
    if level < 4:
        group_df.group = group_df["cath_code"].str.split(".", expand=True)[:level+1].fillna("").agg('.'.join, axis=1)
        map_job(job, process_cath_names_for_group, group_df.groupby("group"), cath_full_h5, level=level+1)

def delete_groups(root):
    if hasattr(root, "keys"):
        for key in root.keys():
            delete_groups(root[key])
            del root[key]
    del root

def process_cath_names(job, cath_full_h5, cathcode=None, skip_cathcode=None, force=False, work_dir=None):
    """Will overwrite all files"""
    if work_dir is None:
        if job is not None and hasattr(job, "fileStore"):
            work_dir = job.fileStore.getLocalTempDir()
        else:
            work_dir = os.getcwd()

    RealtimeLogger.info(f"Creating file {cath_full_h5}")

    if is_num(force) and int(force)==3:
        RealtimeLogger.info(f"Removing all previous data from {cath_full_h5}")
        try:
            #Empty file if it has been created before
            with h5py.File(cath_full_h5, mode="a", use_cache=False, retries=100) as store:
                delete_groups(store)
                store.flush()
                RealtimeLogger.info(f"Deleted {cath_full_h5}")

            with h5py.Folder(os.path.dirname(cath_full_h5)+"/", mode="a") as hparent:
                del hparent[os.path.basename(cath_full_h5)]
        except IOError:
            raise
            #not created
            pass
        except OSError:
            pass
        RealtimeLogger.info(f"Removed all previous data from {cath_full_h5}")
    else:
        RealtimeLogger.info(f"Not deleting any previous data from {cath_full_h5}")

    cath_names_file = os.path.join(work_dir, "cath-names.txt")
    if not os.path.isfile(cath_names_file):
        urllib.request.urlretrieve(
            "ftp://orengoftp.biochem.ucl.ac.uk/cath/releases/latest-release/cath-classification-data/cath-names.txt",
            cath_names_file)

    names = pd.read_csv(cath_names_file, sep="    ", header=None, comment="#",
        names=["cath_code", "representative", "description"])
    names["description"] = names["description"].str[1:]
    names = names.assign(h5_key="/"+names["cath_code"].str.replace(".","/"))
    names = names.assign(group=names["cath_code"].str.split(".", expand=True)[[0,1]].fillna("").agg('.'.join, axis=1))

    if cathcode is not None:
        if not isinstance(cathcode, (list, tuple)):
            cathcode = [cathcode]

        use_sfams = tuple([".".join(sfam)+"." if isinstance(sfam, (list,tuple)) else sfam.replace("/", ".")+"." \
            for sfam in cathcode])

        names_ = None
        for code in use_sfams:
            subset = names.apply(lambda r: r if code.startswith(r["cath_code"]+".") else pd.Series(dtype=str), axis=1).dropna()
            if names_ is None:
                names_ = subset
            else:
                names_ = pd.concat((names_, subset))
        names = names_.drop_duplicates().reset_index(drop=True)
        del names_

    if skip_cathcode is not None:
        if not isinstance(skip_cathcode, (list, tuple)):
            skip_cathcode = [skip_cathcode]

        skip_sfams = tuple([".".join(sfam)+"." if isinstance(sfam, (list,tuple)) else sfam.replace("/", ".")+"." \
            for sfam in skip_cathcode])

        skip_names = None
        for code in use_sfams:
            subset = names.apply(lambda r: r if code.startswith(r["cath_code"]+".") else pd.Series(), axis=1).dropna()
            if skip_names is None:
                skip_names = subset
            else:
                skip_names = pd.concat((skip_names, subset))
        skip_names = skip_names.drop_duplicates().reset_index(drop=True)
        skip_names = pd.merge(names, skip_names, how='inner', on="cath_code").index

        names = names.drop(skip_names)

    root_nodes = names[names["cath_code"].str.split(".").agg(len)==1] #1,2,3,4
    process_cath_names_for_group(job, ("root", root_nodes), cath_full_h5)

    groups = [g for g in names[~names.index.isin(root_nodes.index)].groupby("group")]
    map_job(job, process_cath_names_for_group, groups, cath_full_h5)

    job.addFollowOnJobFn(finish_section, cath_full_h5, "completed_names")

def setup_custom_cath_file_for_sfam(job, full_sfam_path, cath_full_h5):
    with h5py.File(cath_full_h5, mode="a", use_cache=False, retries=100) as store:
        for _, row in group_df.iterrows():
            group = store.require_group(row.h5_key)
            group.description = row.description
            group.representativeDomain = row.representative
            if row.cath_code.count(".") == 3:
                store.require_group(f"{row.h5_key}/domains")

def setup_custom_file(job, cath_full_h5, pdbs, update=False, force=False):
    RealtimeLogger.info(f"Creating file {cath_full_h5}")

    if is_num(force) and int(force)==3:
        RealtimeLogger.info(f"Removing all previous data from {cath_full_h5}")
        try:
            #Empty file if it has been created before
            with h5py.File(cath_full_h5, mode="a", use_cache=False, retries=100) as store:
                delete_groups(store)
                store.flush()
                RealtimeLogger.info(f"Deleted {cath_full_h5}")

            with h5py.Folder(os.path.dirname(cath_full_h5)+"/", mode="a", retries=100) as hparent:
                del hparent[os.path.basename(cath_full_h5)]
        except IOError:
            raise
            #not created
            pass
        except OSError:
            pass
        RealtimeLogger.info(f"Removed all previous data from {cath_full_h5}")
    else:
        RealtimeLogger.info(f"Not deleting any previous data from {cath_full_h5}")

    with h5py.File(cath_full_h5, mode="a", use_cache=False) as store:
        pass

    if isinstance(pdbs, bool) and pdbs:
        #Use entire PDB database
        job.addFollowOnJob(get_all_pdbs, cath_full_h5, update=update)
    elif isinstance(pdbs, (list,tuple)) and isinstance(pdbs[0], str)
        if Path(pdbs[0]).is_file():
            #Ceate custom files, not implemented
            pass
        elif len(pdbs[0]) < 9:
            #Is PDB_entity or PDB.chain or just PDB
            job.addFollowOnJob(get_custom_pdbs, pdbs, cath_full_h5)
    else:
        pdbs = Path(pdbs)
        if pdbs.is_dir():
            child_files = list(pdbs.iterdir())
            if all([f.is_dir() for f in child_files]):
                if all([f.stem.count(".")==3 for f in child_files]):
                    #Are all CATH directories
                    names = pd.DataFrame([(*f.stem.split(), str(f)) for f in child_files], columns=["C", "A", "T", "H", "full_path"])
                    names = names.assign(group=names["cath_code"].str.split(".", expand=True)[[0,1]].fillna("").agg('.'.join, axis=1))

                    with h5py.File(cath_full_h5, mode="a", use_cache=False) as store:
                        for className in names.C.drop_duplicates():
                            #Only max 4
                            store.require_group(className)
                        for class_arch, _ in names.groupby(["C", "A"]):
                            #only max 41
                            store.require_group(f"{class_arch[0]}/{class_arch[1]}")

                    map_job(job, setup_custom_cath_file_for_sfam, child_files, cath_full_h5)
                else:
                    #Follow direcotry structure
                    raise NotImplementedError
            else:
                #All PDB files in single direcotry


def create_h5_hierarchy(job, cath_full_h5, cathcode=None, skip_cathcode=None, pdbs=None, work_dir=None, force=False):
    if work_dir is None:
        if job is not None and hasattr(job, "fileStore"):
            work_dir = job.fileStore.getLocalTempDir()
        else:
            work_dir = os.getcwd()

    if pdbs is not None:
        #Just makes sure file exists, and let the start_domain_and_features in main to add info
        return setup_custom_file(job, cath_full_h5, pdbs, force=force)

    run_names = True

    if isinstance(force, bool) or (is_num(force) and int(force)<3):
        try:
            with h5py.File(cath_full_h5, mode="r", use_cache=False, retries=100) as store:
                run_names = not store.attrs.get("completed_names", False)
        except IOError:
            #Never created, ignore
            pass

    RealtimeLogger.info(f"Force is {force}, run_names={run_names}")

    if run_names:
        job.addChildJobFn(process_cath_names, cath_full_h5, cathcode=cathcode, skip_cathcode=skip_cathcode, force=force)

    job.addFollowOnJobFn(process_cath_domain_list, cath_full_h5, cathcode=cathcode, skip_cathcode=skip_cathcode, force=force)

def finish_section(job, cath_full_h5, attribute):
    with h5py.File(cath_full_h5, mode="a", use_cache=False, retries=100) as store:
        store.attrs[attribute] = True

def is_num(a):
    try:
        int(a)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    force = len(sys.argv)>1 and args[1] in ["-f", "--force"]
