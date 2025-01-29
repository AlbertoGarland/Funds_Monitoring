import os


APP_ENTRY_POINT = 'View_test.py'
#APP_ENTRY_POINT = 'view.py'

dir_path = os.path.dirname(__file__)
path = os.path.join(dir_path, APP_ENTRY_POINT)

os.system('streamlit run "{}"'.format(path))