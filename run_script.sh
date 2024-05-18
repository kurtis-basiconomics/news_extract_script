#!/bin/bash
cd /home/basiconomics_vm_user_01/news_extract_script
source venv/bin/activate
python main.py >> /home/basiconomics_vm_user_01/news_extract_script/script.log 2>&1
