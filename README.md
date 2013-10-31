Please note this is not 100% completed work yet. 
Since we use spacewalk (https://fedorahosted.org/spacewalk/) to patch and manage our RHEL based systems it's an obvious move to have a script
that would patch all of them from a command line. Yes, I’m aware of a spacecmd tool that could do that as well but it doesn’t meet our needs completely. Why?

1. List of the systems to be updated is dynamic and could change from time to time. 
2. It’s distributed as an Excel file.
3. We don’t patch and reboot all systems at once and the order is defined in the aforementioned Excel file by special markers, i.e. UAT1/UAT2, PROD1/PROD2. 
   So basically we do UAT1 first and once we’re sure everything is fine we go on with UAT2.
4. Additional checks and reports are required in the end. 
