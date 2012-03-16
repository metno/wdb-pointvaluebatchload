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

#ifndef ABSTRACTJOB_H_
#define ABSTRACTJOB_H_

#include "DataQue.h"
#include <boost/shared_ptr.hpp>
#include <string>


namespace fastload
{

class AbstractJob
{
public:
	AbstractJob(DataQue::Ptr que);
	virtual ~AbstractJob();

	virtual void operator () ();

	enum Status
	{
		Ready, Running, Done, Error
	};
	Status status() const { return runInformation_->status; }
	std::string errorMessage() const { return runInformation_->errorMessage; }

protected:
	virtual void run() =0;

	DataQue::Ptr que_;

private:
	struct RunInformation
	{
		Status status;
		std::string errorMessage;
	};
	boost::shared_ptr<RunInformation> runInformation_;
};

} /* namespace fastload */
#endif /* ABSTRACTJOB_H_ */
