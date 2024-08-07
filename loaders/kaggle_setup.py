# !pip install kaggle
api_token = {"username":"USERNAME","key":"API_KEY"}
import json
import zipfile
import os
with open('/content/.kaggle/kaggle.json', 'w') as file:
    json.dump(api_token, file)
# !chmod 600 /content/.kaggle/kaggle.json
# !kaggle config path -p /content
# !kaggle competitions download -c jigsaw-toxic-comment-classification-challenge
os.chdir('/content/competitions/jigsaw-toxic-comment-classification-challenge')
for file in os.listdir():
    zip_ref = zipfile.ZipFile(file, 'r')
    zip_ref.extractall()
    zip_ref.close()