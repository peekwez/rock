include Make.in

help:
	@echo "Run 'make <target>' where target is on of the follow:"
	@echo "-----------------------------------------------------"
	@echo " install           Install rock library"
install: clean
	$(call _info, installing rock library...)
	python setup.py install 
	rm -fr build/ dist/
	rm -fr rock.egg-info

clean:
	$(call _info, cleaning after build...)
	python setup.py clean --all


