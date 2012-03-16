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
	("namespace", value( & out.nameSpace), "use the given wdb namespaces");
	return wciBegin;
}
}


Configuration::Configuration()
{
	shownOptions().add(getWciBegin( * this ) );
	cmdOptions().add(getWciBegin( * this ) );
	cmdOptions().add( getInput( * this ) );
	positionalOptions_.add( "name", -1 );
}

Configuration::~Configuration()
{
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
