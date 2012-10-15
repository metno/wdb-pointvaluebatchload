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

/**
 * Parsing and storing command-line options
 */
class Configuration
{
public:
	Configuration(int argc, char ** argv);
	~Configuration();

	/**
	 * File(s) to load
	 */
	const std::vector<std::string> & file() const { return file_; }

	/**
	 * Get the connection string to use with libpq to connect to a wdb database
	 */
	std::string pqConnect() const;

	/**
	 * user for wci.begin()
	 */
	const std::string & wciUser() const { return user_; }

	/**
	 * namespace for wci.begin(). Format is expected to be a single string,
	 * like "88,88,88".
	 */
	const std::string & nameSpace() const { return nameSpace_; }

	/**
	 * Log output file
	 */
	const std::string & logFile() const { return logFile_; }

	/**
	 * Logging level
	 */
	log4cpp::Priority::Value logLevel() const;

	/**
	 * If true, we should only create floatvaluegroups, and not populate
	 * floatvalueitem table.
	 */
	bool onlyCreateCroups() const { return onlyCreateCroups_; }

	/**
	 * Write program's version information to the given stream
	 */
	std::ostream & printVersion(std::ostream & stream);

	/**
	 * Write help information to the given stream
	 */
	std::ostream & printHelp(std::ostream & stream);

	/**
	 * Write help on data format to the given stream
	 */
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

	bool onlyCreateCroups_;
};

} /* namespace fastload */
#endif /* CONFIGURATION_H_ */
