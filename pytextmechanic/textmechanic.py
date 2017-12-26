#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse,os,sys,re
import multiprocessing as mp

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def parse_args():
    outputdir = os.getcwd()+os.sep+"outputtext.txt"
    parser = argparse.ArgumentParser(
    description='Perform text analysis and manipulation.')
    parser.add_argument(
    '-i', '--inputfile', type=str, help='Thread url to download image from', required=True)
    parser.add_argument(
    '-o', '--outputfile', type=str, help='Output file, default %s' %(outputdir), required=False, default=outputdir)
    parser.add_argument(
    '-p', '--prefix', type=str, help='Insert a prefix into the content of each line', required=False, default=None)
    parser.add_argument(
    '-s', '--suffix', type=str, help='Insert a suffix into the content of each line', required=False, default=None)
    parser.add_argument(
    '-e', '--extractcolumn', nargs="*", dest='cols',help='Enter column number to be extracted (1, 2, 3, etc.) and delimiter that seperates columns i.e character, word, segment of html code, etc. Specify parameters in double quotes like "COL1" "DELIMITER".', required=False, default=None)
    parser.add_argument(
    '-r', '--replacetext', nargs="*", dest='replacer',help='Enter text to be replaced with other text. Regular expressions are supported.', required=False, default=None)
    parser.add_argument(
    '-d', '--deleteemptylines',nargs='?',const=True, type=str2bool,help='Use this flag if you want to remove all the empty/blank lines from your file.', required=False, default=False)
    
    #parser.add_argument(
    #'-c', '--count',nargs='?',const=True, type=str2bool, help='Count your text\'s characters, words, sentences, lines and word frequency.', required=False, default=False)
    args = parser.parse_args()
    #print(args)
    inputfile = args.inputfile
    outfile = args.outputfile
    #count = args.count
    suffix = args.suffix
    prefix = args.prefix
    excolumn = args.cols
    replacer = args.replacer
    delete = args.deleteemptylines
    return inputfile,outfile,suffix,prefix,excolumn,replacer,delete
    
class textMechanic():
    def __init__(self,inputfile,outputfile,excol=None,replacer=None,suffix=None,prefix=None,delete=None):
        self.inputfile = inputfile
        self.outputfile = outputfile
        self.prefix = prefix
        self.suffix = suffix
        if delete:
            self.delete = delete
        if replacer:
            self.toreplace = replacer[0]
            self.replacer = replacer[1]
        else:
            self.toreplace = None
            self.replacer = None
        self.excol = excol
        if self.excol and isinstance(excol[0],int):
            self.colnumber = int(self.excol[0])
            self.delimiter = self.excol[1]
        elif self.excol:
            self.colnumber = int(self.excol[1])
            self.delimiter = self.excol[0]
        else:
            self.colnumber = None
            self.delimiter = None
            
    def start(self):
        pool = mp.Pool(10)
        jobs = []

        for chunkStart,chunkSize in self.chunkify():
            jobs.append( pool.apply_async(self.process_wrapper,(chunkStart,chunkSize)) )

        for job in jobs:
            job.get()

        pool.close()
        
    def process(self,line):
        line = str(line)[2:]
        line = line[:-1]
        line = line.strip()
        dlt = False
        """if self.count:
            self.lines.value += 1
            self.words.value += len(line.split())
            self.letters.value += len(line)"""
        if self.delete:
            dlt = self.isEmpty(line)
        if self.replacer and self.toreplace:
            line = self.replace(line)
        if self.excol:
            line = self.separate(line)
        if self.prefix:
            line = self.add_prefix(line)
        if self.suffix:
            line = self.add_suffix(line)
        if (self.prefix or self.suffix or self.excol or (self.replacer and self.toreplace)) and not dlt:
            with open(self.outputfile,'a') as f:
                f.write(line+os.linesep)
        
    def process_wrapper(self,chunkStart, chunkSize):
        with open(self.inputfile,'rb') as f:
            f.seek(chunkStart)
            lines = f.read(chunkSize).splitlines()
            for line in lines:
                self.process(line)

    def chunkify(self,size=1024*1024):
        fileEnd = os.path.getsize(self.inputfile)
        with open(self.inputfile,'rb') as f:
            chunkEnd = f.tell()
            while True:
                chunkStart = chunkEnd
                f.seek(size,1)
                f.readline()
                chunkEnd = f.tell()
                yield chunkStart, chunkEnd - chunkStart
                if chunkEnd > fileEnd:
                    break
    def separate(self,string):
        try:
            return string.split(self.delimiter)[self.colnumber]
        except:
            print("Couldn't extract column at line '%s'. Are you sure that there are %s columns?"%(string,self.colnumber))
    
    def isEmpty(self,string):
        if not string:
            return True
        return False
    
    def add_suffix(self,string):
        return string+self.suffix
        
    def add_prefix(self,string):
        return self.prefix+string
        
    def replace(self,string):
        return re.sub(self.toreplace, self.replacer, string.rstrip())
        
if __name__ == "__main__":
    inputfile,outfile,suffix,prefix,excol,replacer,delete = parse_args()
    print(outfile)
    if excol:
        if len(excol) != 2:
            sys.exit('[FATAL] - If you\'re using --e (extractcolumn) you need to specify 2 parameters, the delimiter and the column number you want to extract, example: (-e ";" "1") where ; is the delimiter and 1 the column number.')
    if replacer:
        if len(replacer) != 2:
            sys.exit('[FATAL] - If you\'re using --r (replacetext) you need to specify 2 parameters, the text to be replaced and the text to replace the old one wit, example: (-r "old" "new").')
    tm = textMechanic(inputfile,outfile,excol,replacer,suffix,prefix,delete)
    print(tm.start())
