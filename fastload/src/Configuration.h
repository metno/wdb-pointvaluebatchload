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

#ifndef CONFIGURATION_H_
#define CONFIGURATION_H_

#include <log4cpp/Priority.hh>
#include <boost/program_options/options_description.hpp>
#include <iosfwd>


namespace fastload
{

class Configuration
{
public:
	Configuration(int argc, char ** argv);
	~Configuration();

	const std::vector<std::string> & file() const { return file_; }

	std::string pqConnect() const;
	const std::string & wciUser() const { return user_; }
	const std::string & nameSpace() const { return nameSpace_; }

	const std::string & logFile() const { return logFile_; }
	log4cpp::Priority::Value logLevel() const;

	std::ostream & printVersion(std::ostream & stream);
	std::ostream & printHelp(std::ostream & stream);
	std::ostream & printFormatHelp(std::ostream & stream);

private:
	boost::program_options::options_description shownOptions_();
	void parseOptions_(int argc, char ** argv);

	std::vector<std::string> file_;

	std::string database_;
	std::string host_;
	int port_;
	std::string user_;

	std::string nameSpace_;

	std::string logFile_;
	int logLevel_;
};

} /* namespace fastload */
#endif /* CONFIGURATION_H_ */
