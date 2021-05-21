# AnalysisServices
Automation repo for TOM engine in AAS and PowerBI 

The portion of this script that allow accessing the TOM engine is thanks to the hard work of yehoshuadimarsky his [ssas_api.py](https://github.com/yehoshuadimarsky/python-ssas/blob/master/ssas_api.py) allowed me to create a read, write,refresh application for working on AAS/PowerBI servers.

Prerequisites:


1-  This script requires 2 dll's to be installed prior to execution and can be downloaded from the [microsoft client libraries](https://docs.microsoft.com/en-us/analysis-services/client-libraries?view=asallproducts-allversions) for .net. You will need the ADOMD and AMO packages installed in your machine.

2-  All 3 scripts must be placed in the same python project folder or you will need to user folder references in your import method.


Notes: 


1-  The main main method for getting data out of TOM is the microsoft .net SDK. This code can be augmented by adding more classes as seen on the documentation for [Microsoft.AnalysisServices.Tabular](https://docs.microsoft.com/en-us/dotnet/api/microsoft.analysisservices.tabular.column?view=analysisservices-dotnet)

2-  I will be cleaning up the code to use classes eventuall to make it cleaner but for now everything has been tested

3-  The date "12/31/1699 12:00:00 AM" you see in the PyTOM.py script is how TOM stores null dates for my purpose i wanted to find partitions that have never been refreshed to i am targetting those dates to send over to my refresh script.



