"""
IOStore class originated here

https://github.com/BD2KGenomics/hgvm-graph-bakeoff-evaluations/blob/master/scripts/toillib.py

and was then here:

https://github.com/cmarkello/toil-lib/blob/master/src/toil_lib/toillib.py

In a perfect world, this would be deprecated and replaced with Toil's stores.

Actually did this here:

https://github.com/glennhickey/toil-vg/tree/issues/110-fix-iostore

But couldn't get Toil's multipart S3 uploader working on large files.  Also,
the toil jobStore interface is a little less clean for our use.

So for now keep as part of toil-vg where it works.  Could also consider merging
into the upstream toil-lib

https://github.com/BD2KGenomics/toil-lib
"""


import sys, os, os.path, json, collections, logging, logging.handlers
try:
    import socketserver
except ImportError:
    import SocketServer #Python 2.7
import struct, socket, threading, tarfile, shutil
import tempfile
import functools
import random
import time
import dateutil
import traceback
import stat
from toil.realtimeLogger import RealtimeLogger
import subprocess
import datetime
import json
import subprocess
import shutil
from pathlib import Path

# Need stuff for Amazon s3
try:
    import boto3
    import botocore
    have_s3 = True
except ImportError:
    have_s3 = False
    pass

def robust_makedirs(directory):
    """
    Make a directory when other nodes may be trying to do the same on a shared
    filesystem.

    """

    if not os.path.exists(directory):
        try:
            # Make it if it doesn't exist
            os.makedirs(directory, mode=0o777)
            os.chmod(directory, 0o777)
        except OSError:
            # If you can't make it, maybe someone else did?
            pass

    # Make sure it exists and is a directory
    assert(os.path.exists(directory) and os.path.isdir(directory))


def write_global_directory(file_store, path, cleanup=False, tee=None, compress=True):
    """
    Write the given directory into the file store, and return an ID that can be
    used to retrieve it. Writes the files in the directory and subdirectories
    into a tar file in the file store.

    Does not preserve the name or permissions of the given directory (only of
    its contents).

    If cleanup is true, directory will be deleted from the file store when this
    job and its follow-ons finish.

    If tee is passed, a tar.gz of the directory contents will be written to that
    filename. The file thus created must not be modified after this function is
    called.

    """

    write_stream_mode = "w"
    if compress:
        write_stream_mode = "w|gz"

    if tee is not None:
        with open(tee, "w") as file_handle:
            # We have a stream, so start taring into it
            with tarfile.open(fileobj=file_handle, mode=write_stream_mode) as tar:
                # Open it for streaming-only write (no seeking)

                # We can't just add the root directory, since then we wouldn't be
                # able to extract it later with an arbitrary name.

                for file_name in os.listdir(path):
                    # Add each file in the directory to the tar, with a relative
                    # path
                    tar.add(os.path.join(path, file_name), arcname=file_name)

        # Save the file on disk to the file store.
        return file_store.writeGlobalFile(tee)
    else:

        with file_store.writeGlobalFileStream(cleanup=cleanup) as (file_handle,
            file_id):
            # We have a stream, so start taring into it
            # TODO: don't duplicate this code.
            with tarfile.open(fileobj=file_handle, mode=write_stream_mode) as tar:
                # Open it for streaming-only write (no seeking)

                # We can't just add the root directory, since then we wouldn't be
                # able to extract it later with an arbitrary name.

                for file_name in os.listdir(path):
                    # Add each file in the directory to the tar, with a relative
                    # path
                    tar.add(os.path.join(path, file_name), arcname=file_name)

            # Spit back the ID to use to retrieve it
            return file_id

def read_global_directory(file_store, directory_id, path):
    """
    Reads a directory with the given tar file id from the global file store and
    recreates it at the given path.

    The given path, if it exists, must be a directory.

    Do not use to extract untrusted directories, since they could sneakily plant
    files anywhere on the filesystem.

    """

    # Make the path
    robust_makedirs(path)

    with file_store.readGlobalFileStream(directory_id) as file_handle:
        # We need to pull files out of this tar stream

        with tarfile.open(fileobj=file_handle, mode="r|*") as tar:
            # Open it for streaming-only read (no seeking)

            # We need to extract the whole thing into that new directory
            
            import os
            
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(tar, path)


class IOStore(object):
    """
    A class that lets you get your input files and save your output files
    to/from a local filesystem, Amazon S3, or Microsoft Azure storage
    transparently.

    This is the abstract base class; other classes inherit from this and fill in
    the methods.

    """

    def __init__(self):
        """
        Make a new IOStore
        """

        raise NotImplementedError()

    def connect(self):
        """
        Connect to Resource
        """

        raise NotImplementedError()


    def read_input_file(self, input_path, local_path):
        """
        Read an input file from wherever the input comes from and send it to the
        given path.

        If the file at local_path already exists, it is overwritten.

        If the file at local_path already exists and is a directory, behavior is
        undefined.

        """

        raise NotImplementedError()

    def list_input_directory(self, input_path, recursive=False,
        with_times=False):
        """
        Yields each of the subdirectories and files in the given input path.

        If recursive is false, yields files and directories in the given
        directory. If recursive is true, yields all files contained within the
        current directory, recursively, but does not yield folders.

        If with_times is True, yields (name, modification time) pairs instead of
        just names, with modification times represented as datetime objects in
        the GMT timezone. Modification times may be None on objects that do not
        support them.

        Gives relative file/directory names.

        """

        raise NotImplementedError()

    def write_output_file(self, local_path, output_path):
        """
        Save the given local file to the given output path. No output directory
        needs to exist already.

        If the output path already exists, it is overwritten.

        If the output path already exists and is a directory, behavior is
        undefined.

        """

        raise NotImplementedError()

    def write_output_directory(self, local_direcotry, output_directory=""):
        for dirpath, dirnames, filenames in os.walk(local_direcotry):
            for f in filenames:
                fpath = os.path.join(dirpath, f)
                key = fpath[len(direcotry):]
                self.write_output_file(fpath, output_directory+key)

    def exists(self, path):
        """
        Returns true if the given input or output file exists in the store
        already.

        """

        raise NotImplementedError()

    def get_mtime(self, path):
        """
        Returns the modification time of the given gile if it exists, or None
        otherwise.

        """

        raise NotImplementedError()

    def get_size(self, path):
        """
        Returns the size in bytes of the given file if it exists, or None
        otherwise.

        """

        raise NotImplementedError()

    def get_number_of_items(self, path):
        """
        Return the number of items in path if it exits, or None otherwise
        """

        raise NotImplementedError()

    def remove_file(self, path):
        """
        Removes the object from the store
        """

        raise NotImplementedError()


    @staticmethod
    def absolute(store_string):
        """
        Convert a relative path IOStore string to an absolute path one. Leaves
        strings that aren't FileIOStore specifications alone.

        Since new Toil versions change the working directory of SingleMachine
        batch system jobs, we need to have absolute paths passed into jobs.

        Recommended to be used as an argparse type, so that strings can be
        directly be passed to IOStore.get on the nodes.

        """

        if store_string == "":
            return ""
        if store_string[0] == ".":
            # It's a relative ./ path
            return os.path.abspath(store_string)
        if store_string.startswith("file:"):
            # It's a file:-prefixed thing that may be a relative path
            # Normalize the part after "file:" (which is 5 characters)
            return "file:" + os.path.abspath(store_string[5:])

        return store_string


    @staticmethod
    def get(store_string, create=True):
        """
        Get a concrete IOStore created from the given connection string.

        Valid formats are just like for a Toil JobStore, except with container
        names being specified on Azure.

        Formats:

        /absolute/filesystem/path

        ./relative/filesystem/path

        file:filesystem/path

        aws:region:bucket (TODO)

        aws:region:bucket/path/prefix (TODO)

        azure:account:container (instead of a container prefix) (gets keys like
        Toil)

        azure:account:container/path/prefix (trailing slash added automatically)

        """

        if isinstance(store_string, (IOStore, FileS3IOStore)):
            return store_string

        # Code adapted from toil's common.py loadJobStore()

        if store_string[0] in "/.":
            # Prepend file: tot he path
            store_string = "file:" + store_string

        try:
            # Break off the first colon-separated piece.
            store_type, store_arguments = store_string.split(":", 1)
        except ValueError:
            # They probably forgot the . or /
            raise RuntimeError("Incorrect IO store specification {}. "
                "Local paths must start with . or /".format(store_string))

        if store_type == "file":
            return FileIOStore(store_arguments, create=create)
        elif store_type == "aws":
            # Break out the AWS arguments
            region, bucket_name = store_arguments.split(":", 1)

            if "/" in bucket_name:
                # Split the bucket from the path
                bucket_name, path_prefix = bucket_name.split("/", 1)
            else:
                # No path prefix
                path_prefix = ""

            return S3IOStore(region, bucket_name, path_prefix, create=create)
        elif store_type == "file-aws":
            # Break out the AWS & File arguments
            region, bucket_name, local_path = store_arguments.split(":")

            if "/" in bucket_name:
                # Split the bucket from the path
                bucket_name, path_prefix = bucket_name.split("/", 1)
            else:
                # No path prefix
                path_prefix = ""

            return FileS3IOStore(region, bucket_name, path_prefix, local_path, create=create)
        else:
            raise RuntimeError("Unknown IOStore implementation {}".format(
                store_type))



class FileIOStore(IOStore):
    """
    A class that lets you get input from and send output to filesystem files.

    """

    def __init__(self, path_prefix="", create=True):
        """
        Make a new FileIOStore that just treats everything as local paths,
        relative to the given prefix.

        """

        self.path_prefix = self.store_name = path_prefix
        self.store_string = "file:"+path_prefix

        path = Path(self.path_prefix)
        if not path.is_dir():
            if create:
                path.mkdir(parents=True, exist_ok=True)
            else:
                raise RuntimeError(f"Not creating FileIOStore at {path_prefix}")


    def read_input_file(self, input_path, local_path):
        """
        Get input from the filesystem.
        """

        RealtimeLogger.debug("Loading {} from FileIOStore in {} to {}".format(
            input_path, self.path_prefix, local_path))

        if os.path.exists(local_path):
            # Try deleting the existing item if it already exists
            try:
                os.unlink(local_path)
            except:
                # Don't fail here, fail complaining about the assertion, which
                # will be more informative.
                pass

        # Make sure the path is clear for copying
        assert(not os.path.exists(local_path))

        # Where is the file actually?
        real_path = os.path.abspath(os.path.join(self.path_prefix, input_path))

        if not os.path.exists(real_path):
            RealtimeLogger.error(
                "Can't find {} from FileIOStore in {}!".format(input_path,
                self.path_prefix))
            raise RuntimeError("File {} missing!".format(real_path))

        # Make a temporary file
        temp_handle, temp_path = tempfile.mkstemp(dir=os.path.dirname(local_path))
        os.close(temp_handle)

        # Copy to the temp file
        shutil.copy2(real_path, temp_path)

        # Rename the temp file to the right place, atomically
        RealtimeLogger.info("rename {} -> {}".format(temp_path, local_path))
        os.rename(temp_path, local_path)

        # Look at the file stats
        file_stats = os.stat(real_path)

        if (file_stats.st_uid == os.getuid() and
            file_stats.st_mode & stat.S_IWUSR):
            # We own this file and can write to it. We don't want the user
            # script messing it up through the symlink.

            try:
                # Clear the user write bit, so the user can't accidentally
                # clobber the file in the actual store through the symlink.
                os.chmod(real_path, file_stats.st_mode ^ stat.S_IWUSR)
            except OSError:
                # If something goes wrong here (like us not having permission to
                # change permissions), ignore it.
                pass

    def download_input_directory(self, prefix, local_dir, postfix=None, force=False):
        if force:
            shutil.copytree(os.path.join(self.path_prefix, prefix), local_dir)
        else:
            os.symlink(os.path.join(self.path_prefix, prefix), local_dir)

    def list_input_directory(self, input_path, recursive=False, with_times=False):
        """
        Loop over directories on the filesystem.
        """

        RealtimeLogger.info("Enumerating {} from "
            "FileIOStore in {}".format(input_path, self.path_prefix))

        if not os.path.exists(os.path.join(self.path_prefix, input_path)):
            # Nothing to list over
            return

        if not os.path.isdir(os.path.join(self.path_prefix, input_path)):
            # Can't list a file, only a directory.
            return

        for item in os.listdir(os.path.join(self.path_prefix, input_path)):
            if(recursive and os.path.isdir(os.path.join(self.path_prefix,
                input_path, item))):
                # We're recursing and this is a directory.
                # Recurse on this.
                for subitem in self.list_input_directory(
                    os.path.join(input_path, item), recursive):

                    # Make relative paths include this directory name and yield
                    # them
                    name_to_yield = os.path.join(item, subitem)

                    if with_times:
                        # What is the mtime in seconds since epoch?
                        mtime_epoch_seconds = os.path.getmtime(os.path.join(
                            input_path, item, subitem))
                        # Convert it to datetime

                        yield name_to_yield, mtime_epoch_seconds
                    else:
                        yield name_to_yield
            else:
                # This isn't a directory or we aren't being recursive
                # Just report this individual item.

                if with_times:
                    # What is the mtime in seconds since epoch?
                    mtime_epoch_seconds = os.path.getmtime(os.path.join(
                        input_path, item))

                    yield item, mtime_epoch_seconds
                else:
                    yield item

    def write_output_file(self, local_path, output_path):
        """
        Write output to the filesystem
        """

        RealtimeLogger.debug("Saving {} to FileIOStore in {}".format(
            output_path, self.path_prefix))

        # What's the real output path to write to?
        real_output_path = os.path.join(self.path_prefix, output_path)

        # What directory should this go in?
        parent_dir = os.path.split(real_output_path)[0]

        if parent_dir != "":
            # Make sure the directory it goes in exists.
            robust_makedirs(parent_dir)

        # Make a temporary file
        temp_handle, temp_path = tempfile.mkstemp(dir=self.path_prefix)
        os.close(temp_handle)
        os.unlink(temp_path)

        try:
            # Copy to the temp file with metadata
            shutil.copy2(local_path, temp_path)
        except PermissionError:
            #copy file to tempfile without metadata
            shutil.copyfile(local_path, temp_path)

        if os.path.exists(real_output_path):
            try:
                # At least try to get existing files out of the way first.
                os.unlink(real_output_path)
            except FileNotFoundError:
                pass

        # Rename the temp file to the right place, atomically
        os.rename(temp_path, real_output_path)

    def exists(self, path):
        """
        Returns true if the given input or output file exists in the file system
        already.

        """

        return os.path.exists(os.path.join(self.path_prefix, path))

    def get_mtime(self, path):
        """
        Returns the modification time of the given file if it exists, or None
        otherwise.

        """

        if not self.exists(path):
            return None

        # What is the mtime in seconds since epoch?
        mtime_epoch_seconds = os.path.getmtime(os.path.join(self.path_prefix,
            path))
        # Convert it to datetime
        mtime_datetime = datetime.datetime.utcfromtimestamp(
            mtime_epoch_seconds).replace(tzinfo=dateutil.tz.tzutc())

        # Return the modification time, timezoned, in UTC
        return mtime_datetime

    def get_size(self, path):
        """
        Returns the size in bytes of the given file if it exists, or None
        otherwise.

        """

        if not self.exists(path):
            return None

        # Return the size in bytes of the backing file
        return os.stat(os.path.join(self.path_prefix, path)).st_size

    def get_number_of_items(self, path=None):
        """
        Return the number of items in path if it is exist, or None otherwise
        """
        if path is not None and self.exists(path):
            return None
        elif path is None:
            path = self.path_prefix

        return sum(1 for name in os.listdir(path) if os.path.isfile(name))

    def remove_file(self, path):
        try:
            os.remove(path)
        except OSError:
            pass

class BackoffError(RuntimeError):
    """
    Represents an error from running out of retries during exponential back-off.
    """

def backoff_times(retries, base_delay):
    """
    A generator that yields times for random exponential back-off. You have to
    do the exception handling and sleeping yourself. Stops when the retries run
    out.

    """

    # Don't wait at all before the first try
    yield 0

    # What retry are we on?
    try_number = 1

    # Make a delay that increases
    delay = float(base_delay) * 2

    while try_number <= retries:
        # Wait a random amount between 0 and 2^try_number * base_delay
        yield random.uniform(base_delay, delay)
        delay *= 2
        try_number += 1

    # If we get here, we're stopping iteration without succeeding. The caller
    # will probably raise an error.

def backoff(original_function, retries=6, base_delay=10):
    """
    We define a decorator that does randomized exponential back-off up to a
    certain number of retries. Raises BackoffError if the operation doesn't
    succeed after backing off for the specified number of retries (which may be
    float("inf")).

    Unfortunately doesn't really work on generators.
    """

    # Make a new version of the function
    @functools.wraps(original_function)
    def new_function(*args, **kwargs):
        last_error = None
        # Call backoff times, overriding parameters with stuff from kwargs
        for delay in backoff_times(retries=kwargs.get("retries", retries),
            base_delay=kwargs.get("base_delay", base_delay)):
            # Keep looping until it works or our iterator raises a
            # BackoffError
            if delay > 0:
                # We have to wait before trying again
                RealtimeLogger.error("Retry after {} seconds".format(
                    delay))
                time.sleep(delay)
            try:
                return original_function(*args, **kwargs)
            except Exception as last_error:
                # Report the formatted underlying exception with traceback
                RealtimeLogger.error("{} failed due to: {}".format(
                    original_function.__name__,
                    "".join(traceback.format_exception(*sys.exc_info()))))


        # If we get here, the function we're calling never ran through before we
        # ran out of backoff times. Give an error.
        if last_error is None:
            raise BackoffError("Ran out of retries calling {}".format(
                original_function.__name__))
        else:
            raise last_error

    return new_function

class S3IOStore(IOStore):
    """
    A class that lets you get input from and send output to AWS S3 Storage.

    """

    def __init__(self, region, bucket_name, name_prefix="", create=True):
        """
        Make a new S3IOStore that reads from and writes to the given
        container in the given account, adding the given prefix to keys. All
        paths will be interpreted as keys or key prefixes.

        """

        # Make sure s3 libraries actually loaded
        assert(have_s3)

        self.region = region
        self.bucket_name = self.store_name = bucket_name
        self.name_prefix = name_prefix
        self.store_string = "aws:{}:{}".format(region, bucket_name)
        if name_prefix != "":
            self.store_string += "/{}".format(name_prefix)
        self.s3 = None
        self.enpoint_url = None

        self.create = create
        if not create:
            self.connect()

    def connect(self):
        self.__connect()

    def __connect(self):
        """
        Make sure we have an S3 Bucket connection, and set one up if we don't.
        Creates the S3 bucket if it doesn't exist.
        """

        if self.s3 is None:
            RealtimeLogger.debug("Connecting to bucket {} in region {}".format(
                self.bucket_name, self.region))
            print("Connecting to bucket {} in region {}".format(
                self.bucket_name, self.region))

            kwds = {}
            kwds["config"] = botocore.client.Config(signature_version='s3v4',
                retries={"max_attempts":20})

            if "S3_ENDPOINT" in os.environ:
                kwds["endpoint_url"] = os.environ["S3_ENDPOINT"]

            if "TOIL_S3_HOST" in os.environ:
                host = os.environ['TOIL_S3_HOST']
                port = os.environ.get('TOIL_S3_PORT', None)
                protocol = 'https'
                if os.environ.get('TOIL_S3_USE_SSL', True) == 'False':
                    protocol = 'http'
                endpoint_url = f'{protocol}://{host}' + f':{port}' if port else ''
                kwds["endpoint_url"] = endpoint_url

            if "endpoint_url" in kwds:
                self.endpoint_url =   kwds["endpoint_url"]

            # Connect to the s3 bucket service where we keep everything
            self.s3 = boto3.client('s3', self.region, **kwds)
            self.s3r = boto3.resource('s3', self.region, **kwds)
            try:
                self.s3.head_bucket(Bucket=self.bucket_name)
            except:
                if self.create:
                    if self.region == 'us-east-1':
                        self.s3.create_bucket(
                            Bucket=self.bucket_name,
                        )
                    else:
                        self.s3.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region},
                        )
                else:
                    raise RuntimeError(f"Not creating S3IOStore at {name_prefix}")

    #@backoff
    def read_input_file(self, input_path, local_path):
        """
        Get input from S3.
        """

        self.__connect()

        RealtimeLogger.debug("Loading {} from S3IOStore".format(
            input_path))

        # Download the file contents.
        self.s3.download_file(self.bucket_name, os.path.join(self.name_prefix, input_path), local_path)

        return local_path

    @backoff
    def list_input_directory(self, input_path=None, recursive=False, with_times=False):
        """
        Yields each of the subdirectories and files in the given input path.

        If recursive is false, yields files and directories in the given
        directory. If recursive is true, yields all files contained within the
        current directory, recursively, but does not yield folders.

        If with_times is True, yields (name, modification time) pairs instead of
        just names, with modification times represented as datetime objects in
        the GMT timezone. Modification times may be None on objects that do not
        support them.

        Gives relative file/directory names.

        """
        self.__connect()

        if with_times:
            get_output = lambda f: (f.key, f.last_modified)
        else:
            get_output = lambda f: f.key

        bucket = self.s3r.Bucket(self.bucket_name).objects.all() if input_path is None \
            else self.s3r.Bucket(self.bucket_name).objects.filter(Prefix=input_path)

        for obj in bucket:
            yield get_output(obj)

    def download_input_directory(self, prefix, local_dir, postfix=None):
        return self.sync_directory(local_dir, local_dir, postfix=postfix, download=True)
    
    def upload_input_directory(self, local_dir, prefix, postfix=None):
        return self.sync_directory(local_dir, local_dir, postfix=postfix, download=False)

    def sync_directory(self, dir1, dir2, postfix=None, download=True):

        from boto.utils import get_instance_metadata
#         instanceMetadata = get_instance_metadata()["iam"]["security-credentials"]["toil_cluster_toil"]
#         RealtimeLogger.info("CRED={}".format(instanceMetadata))
#         os.environ["AWS_ACCESS_KEY_ID"] = instanceMetadata["AccessKeyId"]
#         os.environ["AWS_SECRET_ACCESS_KEY"] = instanceMetadata["SecretAccessKey"]
#         os.environ["AWS_SESSION_TOKEN"] = instanceMetadata["Token"]
#
#         if not os.path.isfile("~/.aws/credentials"):
#             if not os.path.isdir("~/.aws"):
#                 os.makedirs("~/.aws")
#             with open("~/.aws/credentials", "w") as f:
#                 print("""[default]
# aws_access_key_id = {AccessKeyId}
# aws_secret_access_key = {SecretAccessKey}
# """.format(**instanceMetadata))

        subprocess.call([sys.executable, "-m", "pip", "install", "awscli", "--upgrade", "--user"])

        import awscli.clidriver
        driver = awscli.clidriver.create_clidriver()

        if download:
            dir1 = "s3://{}/{}".format(self.bucket_name, dir1)
        else:
            dir2 = "s3://{}/{}".format(self.bucket_name, dir2)

        cmd = ["s3", "sync", dir1, dir2]
        if postfix is not None:
            cmd += ["--exclude=\"*\"", "--include=\"*{}\"".format(postfix)]
        if self.endpoint_url is not None:
            cmd = ["--endpoint-url", self.endpoint_url] + cmd

        rc = driver.main(args=cmd)

        # del os.environ["AWS_ACCESS_KEY_ID"]
        # del os.environ["AWS_SECRET_ACCESS_KEY"]
        # del os.environ["AWS_SESSION_TOKEN"]
        del awscli.clidriver

        if rc != 0:
            assert 0
            subprocess.call(["aws", "configure"])

    def write_output_file(self, local_path, output_path):
        """
        Write output to S3.
        """

        self.__connect()

        RealtimeLogger.debug("Saving {} to S3IOStore".format(
            output_path))

        if os.path.isdir(local_path):
            return self.upload_input_directory(local_path, output_path)

        # Download the file contents.
        #self.s3.upload_file(local_path, self.bucket_name, os.path.join(self.name_prefix, output_path))
        self.s3r.Bucket(self.bucket_name).upload_file(local_path, os.path.join(self.name_prefix, output_path))

    @backoff
    def exists(self, path):
        """
        Returns true if the given input or output file exists in the store
        already.

        """

        self.__connect()

        try:
            self.s3.get_object(Bucket=self.bucket_name, Key=path)
            return True
        except (SystemExit, KeyboardInterrupt):
            raise
        except:
            return False

    def get_mtime(self, path):
        """
        Returns the modification time of the given file if it exists, or None
        otherwise.

        """

        raise NotImplementedError()

    def get_size(self, path):
        """
        Returns the size in bytes of the given file if it exists, or None
        otherwise.

        """
        self.__connect()
        return self.s3.head_object(Bucket=self.bucket_name, Key=path)['ContentLength']

    @backoff
    def get_number_of_items(self, path=None):
        """
        Return the number of items in path if it exits, or None otherwise
        """
        self.__connect()
        return sum(1 for _ in self.list_input_directory(path))

    def remove_file(self, path):
        self.__connect()
        self.s3r.Object(self.bucket_name, path).delete()

class FileS3IOStore(object):
    """
    A class that lets you get input from and send output to AWS S3 Storage but
    checks local filesystem first

    """

    def __init__(self, region, bucket_name, name_prefix="", file_path_dir="", create=True):
        """
        Make a new S3IOStore that reads from and writes to the given
        container in the given account, adding the given prefix to keys. All
        paths will be interpreted as keys or key prefixes.

        """
        self.FileIOStore = FileIOStore(os.path.join(file_path_dir, bucket_name), create=create)
        self.S3IOStore = S3IOStore(region, bucket_name, name_prefix, create=create)
        self.path_prefix = os.path.join(file_path_dir, bucket_name)
        self.region = region
        self.bucket_name = self.store_name = bucket_name
        self.name_prefix = name_prefix
        self.store_string = "file-aws:{}:{}".format(region, bucket_name)
        if name_prefix != "":
            self.store_string += "/{}".format(name_prefix)
        self.store_string += ":{}".format(file_path_dir)


    #@backoff
    def read_input_file(self, input_path, local_path):
        """
        Get input from S3.
        """
        try:
            return self.FileIOStore.read_input_file(input_path, local_path)
        except RuntimeError:
            self.S3IOStore.read_input_file(input_path, local_path)
            self.FileIOStore.write_output_file(local_path, input_path)

        return local_path

    @backoff
    def list_input_directory(self, input_path=None, recursive=False, with_times=False):
        """
        Yields each of the subdirectories and files in the given input path.

        If recursive is false, yields files and directories in the given
        directory. If recursive is true, yields all files contained within the
        current directory, recursively, but does not yield folders.

        If with_times is True, yields (name, modification time) pairs instead of
        just names, with modification times represented as datetime objects in
        the GMT timezone. Modification times may be None on objects that do not
        support them.

        Gives relative file/directory names.

        """
        return self.S3IOStore.list_input_directory(input_path=input_path,
            recursive=recursive, with_times=with_times)

    def download_input_directory(self, prefix, local_dir=None, postfix=None):
        self.S3IOStore.download_input_directory(path, self.FileIOStore.path_prefix)
        if local_dir is not None:
            pass

    def write_output_file(self, local_path, output_path):
        """
        Write output to S3.
        """
        self.FileIOStore.write_output_file(local_path, output_path)
        self.S3IOStore.write_output_file(local_path, output_path)

    @backoff
    def exists(self, path):
        """
        Returns true if the given input or output file exists in the store
        already.

        """
        exists_file = self.FileIOStore.exists(path)
        exists_s3 = self.S3IOStore.exists(path)

        if exists_s3 and not exists_file:
            local_path = os.path.join(self.path_prefix, path)
            robust_makedirs(os.path.dirname(local_path))
            self.S3IOStore.read_input_file(path, local_path)

        return exists_s3

    def get_mtime(self, path):
        """
        Returns the modification time of the given file if it exists, or None
        otherwise.

        """

        raise NotImplementedError()

    def get_size(self, path):
        """
        Returns the size in bytes of the given file if it exists, or None
        otherwise.

        """
        self.S3IOStore.get_size(path)

    @backoff
    def get_number_of_items(self, path=None):
        """
        Return the number of items in path if it exits, or None otherwise
        """
        self.S3IOStore.get_number_of_items(path)

    def remove_file(self, path):
        self.FileIOStore.remove_file(path)
        self.S3IOStore.remove_file(path)
