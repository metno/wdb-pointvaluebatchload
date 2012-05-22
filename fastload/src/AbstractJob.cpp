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

#include "AbstractJob.h"
#include <iostream>

namespace fastload
{

AbstractJob::AbstractJob(DataQue::Ptr que) :
		que_(que),
		runInformation_(new RunInformation)
{
	runInformation_->status = Ready;
}

AbstractJob::~AbstractJob()
{
}

void AbstractJob::operator () ()
{
	runInformation_->status = Running;

	try
	{
		run();
		runInformation_->status = Done;
	}
	catch ( std::exception & e )
	{
		runInformation_->status = Error;
		runInformation_->errorMessage = e.what();
		que_->shutdown();
	}
}

} /* namespace fastload */
