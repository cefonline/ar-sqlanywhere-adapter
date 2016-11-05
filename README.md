Please note:
============

Just could not get binary data to work as a bind parameter even when using the correct format
by escaping the data as per comments in quoting.rb:
SQL Anywhere requires that binary values be encoded as \xHH, where HH is a hexadecimal number
It ALWAYS treated the value as a string no matter what I tried. I added -zr all -zo reqlog.txt
option to my server to log exactly what SQLA was processing. I think the reason is that the set_value
method receives the value as a string and then sets the type as a string so SQLA then treats whatever
we pass in as a string.

To fix this problem I've had to modify a fork of sqlanywhere gem working on branch ruby22. I modified
the code to allow me to get/set the bind param. This allowed me to override the type set by set_value
before I bound the param to the statement, see code below. Setting the type means we pass in the binary
data as is without encoding the value at all (see quoting.rb)

The problem with this approach is that it binds this gem to a particular branch of sqlanywhere (ruby22).

If anyone has a better approach please let me know.


SQL Anywhere ActiveRecord Driver
================================

This is a SQL Anywhere driver for Ruby ActiveRecord. This driver requires the
native SQL Anywhere Ruby driver. To get the native driver, use:

<pre>
   gem install sqlanywhere
</pre>

This driver is designed for use with ActiveRecord 3.0.3 and greater.

This driver is licensed under the Apache License, Version 2.

Making a Connection
-------------------

The following code is a sample database configuration object.

<pre>
  ActiveRecord::Base.configurations = {
    'arunit' => {
      :adapter  => 'sqlanywhere', 
      :database => 'arunit',       #equivalent to the "DatabaseName" parameter
      :server   => 'arunit',       #equivalent to the "ServerName" parameter
      :username => 'dba',          #equivalent to the "UserID" parameter
      :password => 'sql',          #equivalent to the "Password" parameter
      :encoding => 'Windows-1252', #equivalent to the "CharSet" parameter
      :commlinks => 'TCPIP()',     #equivalent to the "Commlinks" parameter
      :connection_name => 'Rails'  #equivalent to the "ConnectionName" parameter
  }
</pre>

Running the ActiveRecord Unit Test Suite
----------------------------------------

1. Download https://github.com/ccouzens/rails and checkout the sqlanywhere_testing branch.

2. Create the two test databases. These can be created in any directory.

<pre>
      dbinit -c arunit
      dbinit -c arunit2
      dbsrv11 arunit arunit2
</pre>

   <b>If the commands cannot be found, make sure you have set up the SQL Anywhere environment variables correctly.</b> For more information review the online documentation [here](http://dcx.sybase.com/index.php#http%3A%2F%2Fdcx.sybase.com%2F1100en%2Fdbadmin_en11%2Fda-envvar-sect1-3672410.html).

3. Enter the custom Rails repository.

4. Run bundle with SQLANYWHERE

<pre>
      SQLANYWHERE=sqlanywhere bundle
</pre>

5. Enter the activerecord directory.

6. Run the unit test suite from the ActiveRecord install directory:

<pre>
      SQLANYWHERE=sqlanywhere rake test_sqlanywhere
</pre>

   <b>If the migration tests fail, make sure you have set up the SQL Anywhere environment variables correctly.</b> For more information review the online documentation [here](http://dcx.sybase.com/index.php#http%3A%2F%2Fdcx.sybase.com%2F1100en%2Fdbadmin_en11%2Fda-envvar-sect1-3672410.html).
