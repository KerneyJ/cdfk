source venv/bin/activate

pip3 uninstall cdfk -y
python3 setup.py build
python3 setup.py install
