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

#ifndef DATAQUE_H_
#define DATAQUE_H_

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <deque>
#include <string>

namespace fastload
{

class DataQue : boost::noncopyable
{
public:
	explicit DataQue(unsigned maxQueSize = 500000, const std::string & queName = std::string());
	~DataQue();

	typedef boost::shared_ptr<DataQue> Ptr;

	typedef std::string Data;

	bool get(Data & out);
	void put(const Data & element);

	void done();

	unsigned callsToPut() const
	{
		return inserts_;
	}

	unsigned callsToGet() const
	{
		return extracts_;
	}

private:
	std::deque<Data> que_;
	bool done_;
	boost::mutex mutex_;
	boost::condition condition_;

	unsigned maxQueSize_;
	std::string queName_;

	unsigned inserts_;
	unsigned extracts_;
};

} /* namespace fastload */
#endif /* DATAQUE_H_ */
