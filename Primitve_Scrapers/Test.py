import vobject
import pprint

vcf_file = open('C:\\Users\\James\\Downloads\\Atif_Ahmad.vcf', 'r')
vcf_string = vcf_file.read()

vc = vobject.readOne(vcf_string)

x = vc.contents
print(x['n'][0].contents)
print(type(x['n'][0]))
