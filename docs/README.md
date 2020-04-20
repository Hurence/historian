Building the documentation is simple. You must be in the docs folder and launch the maven process using this command:

``mvn process-resources``

Currently only the french documentation is built. 

The process for editing is the following: the userguide is edited in asciidoc in all supported languages in a 
sub-directory corresponding to the language locale. Asciidoctor processors are then used to take the asciidoc 
files and generate the other formats. The maven process generates html5 and pdf versions at the moment. 
Future versions will support other languages than French as well as the DocBook and EPub formats.

# Dev

If you are editing the doc and want to test if its work, you can run only your doc generation with this command

```shell script
mvn clean <plugin-prefix>:<plugin-goal>@<execution-id>
```

Fir example if i want to only generate the french user guide in pdf I would execute

```shell script
mvn clean asciidoctor:process-asciidoc@fr-usermanual-to-pdf
```
