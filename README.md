tonnerre-python
===============

Tonnerre provides simple messaging between applications. By default, the
messages are transferred using TCP/IP sockets. As such, the sender and
receiver can be on the same or different machines. 2 types are supported
for the message payload: key/value pairs or raw strings. Raw string payloads
are useful for transferring JSON or XML.

License
-------
BSD

Dependencies
------------
Chaudière - Python core functionality with sockets, threading, configuration, etc.

Configuration File
------------------
Tonnerre uses an .INI for configuration. See 'What's an INI file'
if you're not familiar with them. The .INI format was chosen
because it's very simple and I like simple.

Platforms/Tools
---------------
This project is targeted at 2.x versions of Python.

What's An INI File
------------------
In the 'old days', Windows computers made extensive use of INI files.
This was before the system registry came along. An INI file is a
simple text file that is composed of sections, and each section
can have keys and values.

Meaning of Tonnerre
-------------------
What does 'Tonnerre' mean?  It's a French word that means "thunder" or
"thunder clap". My grandfather used to frequently say "Tonnerre!" as
an expression of astonishment (similar to how people say "Really!").

