# This Python file uses the following encoding: utf-8

# if __name__ == "__main__":
#     pass
import os
import shutil
import boto3
import glob
import time

CACHE_NUMBER = 3

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
    s3_client = boto3.client('s3')
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
    aws_access_key_id='AKIAZGGUW7D4DGBFG446',
    aws_secret_access_key='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'
)

s3_client = boto3.client(
    service_name='s3',
    region_name='us-east-2',
    aws_access_key_id='AKIAZGGUW7D4DGBFG446',
    aws_secret_access_key='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'
)

# if os.path.exists("tmp_im"):
#     shutil.rmtree("tmp_im")
# if os.path.exists("tmp_mask"):
#     shutil.rmtree("tmp_mask")
# os.mkdir("tmp_im")
# os.mkdir("tmp_mask")

output_masks = {}

curr_ctr = 0
for obj in s3.Bucket('torngats').objects.all():
    file_path = obj.key
    if 'Images' not in file_path:
        continue

    if ".JPG" not in file_path:
        continue

    split_path = file_path.split("/")

    curr_path = ""
    ctr = 0
    for folder in split_path:
        if ctr == 0:
            ctr += 1
            continue

        if '.JPG' in folder:
            continue

        curr_path += folder
        curr_path += "/"

    curr_camera = split_path[2]
    curr_camera = curr_camera.replace(" ", "")

    im_name = split_path[-1]
    im_name = im_name[:im_name.rfind(".")]

    curr_path += im_name

    mask_path = "Masks/" + curr_path + ".png"
    im_path = "Images/" + curr_path + ".JPG"
    cleaned_path = "Cleaned_Status/" + curr_path + ".txt"

    try:
        s3_client.head_object(Bucket='torngats', Key=cleaned_path)
        continue
    except:
        pass

    with open('tmp_im.txt', 'w') as f:
        f.write("Active")

    # upload_file('tmp_im.txt', 'torngats', object_name=cleaned_path)

    s3_client.download_file('torngats', mask_path, "tmp_mask/" + curr_camera + "_" + im_name + ".png")
    s3_client.download_file('torngats', im_path, "tmp_im/" + curr_camera + "_" + im_name + ".png")

    output_path = "Masks_Clean/" + curr_path + ".png"
    output_masks["tmp_output/" + curr_camera + "_" + im_name + ".png"] = output_path

    curr_ctr += 1

    print(output_masks)
    print(curr_ctr)

    if curr_ctr < CACHE_NUMBER:
        continue

    filenames = glob.glob("tmp_output" + "/*.png")
    print(output_masks)

    for filename in filenames:
        curr_upload_mask = output_masks[filename]
        # upload_file(filename, 'torngats', object_name=curr_upload_mask)
        im_name = filename[filename.find("/")+1:filename.rfind(".")]
        del output_masks[filename]

        os.remove("tmp_im/" + im_name + ".png")
        os.remove("tmp_mask/" + im_name + ".png")
        os.remove("tmp_output/" + im_name + ".png")

    while(1):
        filenames = glob.glob("tmp_output" + "/*.png")
        if len(filenames) > 0:
            break
        time.sleep(5)



    # break

    # curr_ctr += 1

    # if curr_ctr <= 5:

    

# while (1):
    # input = "tmp_output"
    # filenames = glob.glob(input + "/*.png") + glob.glob(input + "/*.jpg") + glob.glob(input + "/*.JPG")
