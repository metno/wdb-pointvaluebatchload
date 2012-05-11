/*
 fastload

 Copyright (C) 2012 met.no

 Contact information:
 Norwegian Meteorological Institute
 Box 43 Blindern
 0313 OSLO
 NORWAY
 E-mail: post@met.no

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 MA  02110-1301, USA
 */

#include "Configuration.h"
#include <boost/program_options.hpp>


using namespace boost::program_options;


namespace fastload
{
namespace
{
options_description
getInput( Configuration & out )
{
    options_description input( "Input" );
    input.add_options()
    ( "name", value( & out.file ), "Name of file to process" )
    ;

	return input;
}

options_description
getWciBegin( Configuration & out )
{
	options_description wciBegin( "wci" );
	wciBegin.add_options()
	("namespace", value( & out.nameSpace), "Use the given wdb namespaces, instead of what is specified in the given file(s)");
	return wciBegin;
}

//options_description
//getErrorHandling( Configuration & out )
//{
//	options_description errorHandling("Error handling");
//	errorHandling.add_options()
//			("all-or-nothing", bool_switch(& out.allOrNothing), "Do not load any data if one row fails to load");
//	return errorHandling;
//}
}


Configuration::Configuration()
{
	shownOptions().add(getWciBegin( * this ) );
	cmdOptions().add(getWciBegin( * this ) );
//	shownOptions().add(getErrorHandling( * this ) );
//	cmdOptions().add(getErrorHandling( * this ) );
	cmdOptions().add( getInput( * this ) );
	positionalOptions_.add( "name", -1 );
}

Configuration::~Configuration()
{
}

std::ostream & Configuration::printFormatHelp(std::ostream & s)
{
	s <<			"Input format for loading point data into wdb\n"
					"\n"
					"When loading data, you need to follow the format described here:\n"
					"\n"
					"All files consists of sections, starting with a data provider name on\n"
					"one line, optionally followed by a tab character and a three-part namespace\n"
					"specification. Data listings should follow on the next lines, with no blank\n"
					"lines between data elements.\n"
					"\n"
					"The \"current\" data provider is reset after a blank line has been\n"
					"encountered. The next line is then expected to contain the name of the\n"
					"next data provider to use.\n"
					"\n"
					"The data listings follow the same pattern as the wci.write function\n"
					"call parameters, with a few exceptions: It should contain instructions\n"
					"for inserting a single tuple on each line. The elements on each line\n"
					"should not contain any quotation marks. Also, the separator between\n"
					"fields should be a single tab character.\n"
					"\n"
					"Here is the ordering of elements in the data list:\n"
					"\n"
					"  * value \n"
					"  * place name \n"
					"  * reference time \n"
					"  * valid from \n"
					"  * valid to\n"
					"  * value parameter name\n"
					"  * level parameter name\n"
					"  * level from\n"
					"  * level to\n"
					"  \n"
					"Optionally, you may also add the following entries (they will be 0 if \n"
					"not given):\n"
					"\n"
					"  * dataversion\n"
					"  * maxdataversion\n"
					"  \n"
					"Note that for all times, it is neccessary to explicitly specify tile zone. \n"
					"Otherwise you may experience some problems with loading.\n";

	return s;
}


void
Configuration::parse_( int argc, char ** argv )
{
	options_description opt;
	opt.add( cmdOptions() ).add( configOptions() );

	store( command_line_parser( argc, argv ).
				options( opt ).
				positional( positionalOptions_ ).
				run(),
			givenOptions_ );
}


} /* namespace fastload */
