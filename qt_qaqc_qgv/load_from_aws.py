# This Python file uses the following encoding: utf-8

# if __name__ == "__main__":
#     pass
import os
import shutil
import boto3
from argparse import ArgumentParser
import glob
import time
import tempfile
import shutil
from botocore.exceptions import ClientError

KEY_FILE = "../qt_qaqc_qgv/aws_key.csv"
with open(KEY_FILE, 'r') as f:
    lines = f.readlines()
    key_id = lines[1]
    key_id = key_id.split(",")
    key_secret = key_id[3]
    key_id = key_id[2]

print(key_id)
print(key_secret)

f.close()

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id=key_id,
        aws_secret_access_key=key_secret
    )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def folder_exists(bucket:str, path:str) -> bool:
    '''
    Folder should exists.
    Folder could be empty.
    '''
    s3 = boto3.client('s3')
    path = path.rstrip('/')
    resp = s3.list_objects(Bucket=bucket, Prefix=path, Delimiter='/',MaxKeys=1)
    return 'CommonPrefixes' in resp

s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id=key_id,
    aws_secret_access_key=key_secret
)

s3_client = boto3.client(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id=key_id,
    aws_secret_access_key=key_secret
)

parser = ArgumentParser()
parser.add_argument("-i", "--input", dest="user_id",
                    help="user id", metavar="FILE")

args = parser.parse_args()
print("arguments are ", args)

curr_id = args.user_id
divided_path = "Tasks/Clean/Divided"

divided_path = divided_path + "/" + curr_id + "/"
all_tasks = s3.Bucket('torngats').objects.filter(Prefix=divided_path)

if os.path.exists("../qt_qaqc_qgv" + "/" +"tmp_im"):
    shutil.rmtree("../qt_qaqc_qgv" + "/" +"tmp_im")
if os.path.exists("../qt_qaqc_qgv" + "/" +"tmp_mask"):
    shutil.rmtree("../qt_qaqc_qgv" + "/" +"tmp_mask")
if os.path.exists("../qt_qaqc_qgv" + "/" +"tmp_output"):
    shutil.rmtree("../qt_qaqc_qgv" + "/" +"tmp_output")
if os.path.exists("../qt_qaqc_qgv" + "/" +"tasks"):
    shutil.rmtree("../qt_qaqc_qgv" + "/" +"tasks")
if os.path.exists("../qt_qaqc_qgv" + "/" +"tasks_complete"):
    shutil.rmtree("../qt_qaqc_qgv" + "/" +"tasks_complete")

os.mkdir("../qt_qaqc_qgv" + "/" +"tmp_im")
os.mkdir("../qt_qaqc_qgv" + "/" +"tmp_mask")
os.mkdir("../qt_qaqc_qgv" + "/" +"tmp_output")
os.mkdir("../qt_qaqc_qgv" + "/" +"tasks")
os.mkdir("../qt_qaqc_qgv" + "/" +"tasks_complete")

mask_dict = {}
im_dict = {}

prev_text = ""
download_file = True

print("Starting up loop...")

while(1):
    for object_summary in all_tasks:
        key = object_summary.key
        print("Key : ", key)
        if ".txt" not in key:
            continue

        key_split = key.split("/")

        # tmp = tempfile.NamedTemporaryFile()

        if download_file:
            if key_split[-1] == prev_text:
                continue

            txt_file = "../qt_qaqc_qgv" + "/" + "tasks/" + key_split[-1]
            prev_text = key_split[-1]

            tmp_txt = key_split[-1]
            print("Downloading text file : ", tmp_txt)

            s3_client.download_file('torngats', key, tmp_txt)

            print("Finished downloading text file : ", tmp_txt)

            download_file = False

            with open(txt_file, 'w') as f_write:
                with open(tmp_txt, 'r') as f_read:
                    for line in f_read.readlines():
                        line = line.strip()
                        line_split = line.split("/")
                        dataset = line_split[1]
                        camera = line_split[2]
                        im_name = line_split[3][:line_split[3].find(".")]
                        f_write.write(camera.replace(" ", "") + "_" + im_name + "\n")

            f_write.close()

            # with open(txt_file, 'w') as f_write:
            with open(tmp_txt, 'r') as f_read:
                for line in f_read.readlines():
                    line = line.strip()
                    line_split = line.split("/")
                    dataset = line_split[1]
                    camera = line_split[2]
                    im_name = line_split[3][:line_split[3].find(".")]

                    im_path = line
                    mask_path = "Masks/" + dataset + "/" + camera + "/" + im_name + ".png"

                    print("Downloading image : ", im_name)

                    local_im_path = "../qt_qaqc_qgv" + "/" + "tmp_im/" + camera.replace(" ", "") + "_" + im_name + ".jpg"
                    local_mask_path = "../qt_qaqc_qgv" + "/" + "tmp_mask/" + camera.replace(" ", "") + "_" + im_name + ".png"

                    s3_client.download_file('torngats', mask_path, local_mask_path)
                    s3_client.download_file('torngats', im_path, local_im_path)

                    print("Finished downloading image : ", im_name)
                    # f_write.write(camera.replace(" ", "") + "_" + im_name + "\n")

                    mask_dict[camera.replace(" ", "") + "_" + im_name] = mask_path
                    im_dict[camera.replace(" ", "") + "_" + im_name] = im_path

                    filenames = glob.glob("../qt_qaqc_qgv" + "/" + "tasks_complete" + "/*.txt")
                    remove_indices = []
                    i = 0

                    # tasks_filenames = glob.glob("../qt_qaqc_qgv" + "/" + "tasks" + "/*.txt")

                    for filename in filenames:
                        with open(filename, 'a') as f:
                            f.write("Labeler: " + curr_id)
                        f.close()

                        finished_path = "Tasks/Clean/Complete/" + filename[filename.rfind("/")+1:]
                        old_path = "Tasks/Clean/Divided/" + curr_id + "/" + filename[filename.rfind("/")+1:]
                        # os.remove(filename)
                        remove_indices.append(i)
                        i += 1
                        upload_file(filename, curr_id.lower(), object_name=finished_path)
                        s3.Object('torngats', old_path).delete()
                    for index in remove_indices:
                        os.remove(filenames[index])

                    filenames = glob.glob("../qt_qaqc_qgv" + "/" + "tmp_output" + "/*.png")
                    if len(filenames) == 0:
                        pass
                        # print("waiting for output...")
                    else:
                        for filename in filenames:
                            split_filename = filename.split("/")

                            im_name = split_filename[3][:split_filename[3].rfind(".")]
                            mask_upload_path = mask_dict[im_name]

                            mask_upload_path_split = mask_upload_path.split("/")

                            upload_file(filename, curr_id.lower(), mask_upload_path)
                            im_path = "../qt_qaqc_qgv" + "/" + "tmp_im/" + im_name + ".jpg"
                            mask_path = "../qt_qaqc_qgv" + "/" + "tmp_mask/" + im_name + ".png"
                            output_path = "../qt_qaqc_qgv" + "/" + "tmp_output/" + im_name + ".png"

                            # time.sleep(1)

                            os.remove(im_path)
                            os.remove(mask_path)
                            os.remove(output_path)

                            f_read.close()
                            # f_write.close()

    filenames = glob.glob("../qt_qaqc_qgv" + "/" + "tmp_output" + "/*.png")
    if len(filenames) == 0:
        pass
        # print("waiting for output...")
    else:
        for filename in filenames:
            split_filename = filename.split("/")

            im_name = split_filename[3][:split_filename[3].rfind(".")]
            mask_upload_path = mask_dict[im_name]

            upload_file(filename, curr_id.lower(), mask_upload_path)
            im_path = "../qt_qaqc_qgv" + "/" + "tmp_im/" + im_name + ".jpg"
            mask_path = "../qt_qaqc_qgv" + "/" + "tmp_mask/" + im_name + ".png"
            output_path = "../qt_qaqc_qgv" + "/" + "tmp_output/" + im_name + ".png"

            # time.sleep(1)

            os.remove(im_path)
            os.remove(mask_path)
            os.remove(output_path)

    filenames = glob.glob("../qt_qaqc_qgv" + "/" + "tmp_im" + "/*.jpg")

    tasks_filenames = glob.glob("../qt_qaqc_qgv" + "/" + "tasks" + "/*.txt")
    if not len(tasks_filenames) >= 2:
        if len(filenames) < 3:
            # print("Next Task!")
            download_file = True
            continue

    filenames = glob.glob("../qt_qaqc_qgv" + "/" + "tasks_complete" + "/*.txt")
    remove_indices = []
    i = 0
    # if len(filenames) > 0:
    #     time.sleep(3)
    for filename in filenames:
        with open(filename, 'a') as f:
            f.write("Labeler: " + curr_id)
        f.close()

        finished_path = "Tasks/Clean/Complete/" + filename[filename.rfind("/")+1:]
        old_path = "Tasks/Clean/Divided/" + curr_id + "/" + filename[filename.rfind("/")+1:]
        # os.remove(filename)
        remove_indices.append(i)
        i += 1
        upload_file(filename, curr_id.lower(), object_name=finished_path)
        s3.Object('torngats', old_path).delete()
    for index in remove_indices:
        os.remove(filenames[index])



    time.sleep(5)

#curr_ctr = 0
#for obj in s3.Bucket('torngats').objects.all():
#    file_path = obj.key
#    if 'Images' not in file_path:
#        continue

#    if ".JPG" not in file_path:
#        continue

#    split_path = file_path.split("/")

#    curr_path = ""
#    ctr = 0
#    for folder in split_path:
#        if ctr == 0:
#            ctr += 1
#            continue

#        if '.JPG' in folder:
#            continue

#        curr_path += folder
#        curr_path += "/"

#    curr_camera = split_path[2]
#    curr_camera = curr_camera.replace(" ", "")

#    im_name = split_path[-1]
#    im_name = im_name[:im_name.rfind(".")]

#    curr_path += im_name

#    mask_path = "Masks/" + curr_path + ".png"
#    im_path = "Images/" + curr_path + ".JPG"
#    cleaned_path = "Cleaned_Status/" + curr_path + ".txt"

#    try:
#        s3_client.head_object(Bucket='torngats', Key=cleaned_path)
#        continue
#    except:
#        pass

#    with open('tmp_im.txt', 'w') as f:
#        f.write("Active")

#    upload_file('tmp_im.txt', 'torngats', object_name=cleaned_path)

#    s3_client.download_file('torngats', mask_path, "tmp_mask/" + curr_camera + "_" + im_name + ".png")
#    s3_client.download_file('torngats', im_path, "tmp_im/" + curr_camera + "_" + im_name + ".png")

    # curr_ctr += 1

    # if curr_ctr <= 5:



# while (1):
    # input = "tmp_output"
    # filenames = glob.glob(input + "/*.png") + glob.glob(input + "/*.jpg") + glob.glob(input + "/*.JPG")
