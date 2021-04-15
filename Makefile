TOP := /var/spool/noaa-gts

BINABS := $(wildcard $(TOP)/staging/*.bin)
#BINFILES :=  $(notdir $(BINABS))

TIMESTAMPS := $(patsubst %,%.processed, $(BINABS))
INCOMING := $(TOP)/incoming
BUFRNOAA := /usr/local/bin/bufrnoaa


default : $(TIMESTAMPS)


%.processed : %
	@(cd $(INCOMING); $(BUFRNOAA) -l -i $< -T U -U JKSW -f $(notdir $<))
	@touch $<.processed

clean:
	@rm -f $(TOP)/staging/*.processed
	@rm -f $(TOP)/incoming/*.bufr


# bufrnoaa -h
# Usage:
# bufrnoaa -i input_file [-h][-f][-l][-F prefix][-T T2_selection][-O selo][-S sels][-U selu]
#    -h Print this help
#    -i Input file. Complete input path file for NOAA *.bin bufr archive file
#    -2 Input file is formatted in alternative form: Headers has '#' instead of '*' marks and no sep after '7777'
#    -l list the names of reports in input file
#    -f Extract selected reports and write them in files, one per bufr message, as
#       example '20110601213442_ISIE06_SBBR_012100_RRB.bufr'. First field in name is input file timestamp
#       Other fields are from header
#    -F prefix. Builds an archive file with the same format as NOAA one but just with selected messages
#       witgh option  -T. Resulting name is 'prefix_input_filename'
#       If no -F option no archive bin file is created.
#       If no message is selected, the a void file is created.
#       File timestamp is the same than input file
#    -T T2_selection. A string with selection. A character per type (T2 code)
#       'S' = Surface . 'O'= Oceanographic. 'U'= upper air
#       If no -T argument then nothing is selected
#    -S sels. String with selection for A1 when T2='S'
#       By default all A1 are selected
#    -O selo. String with selection for A1 when T2='O'
#       By default all A1 are selected
#    -U sels. String with selection for A1 when T2='U'
#       By default all A1 are selected
