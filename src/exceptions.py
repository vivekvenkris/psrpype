

class IncorrectFileHeaderException(Exception):
    def __init__(self, message=None, filename=None):

    	 if message is None:
    	 	if filename is not None:
    			self.message = "Incorrect header for file: " + filename
    		else:
                self.message = "Incorrect header in input file"
            
        else:
        	
            self.message = message

        super().__init__(self.message)
