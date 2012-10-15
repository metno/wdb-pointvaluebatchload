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

/**
 * A simple queue for strings.
 */
class DataQueue : boost::noncopyable
{
public:
	/**
	 * @param maxQueSize The maximum size of the queue at any given time
	 * @param queName a name for debugging purposes
	 */
	explicit DataQueue(unsigned maxQueSize = 500000, const std::string & queName = std::string());
	~DataQueue();

	typedef boost::shared_ptr<DataQueue> Ptr;

	typedef std::string Data;

	/**
	 * Extract a single element from the queue.
	 *
	 * If the queue is empty, block until at least one element has been added,
	 * or someone calls done() or shutdown() on it.
	 *
	 * @param out Return data is placed here
	 *
	 * @throws std::runtime_error if attempting to get an element, and
	 *         shutdown() has been called before an element was ready to
	 *         return.
	 *
	 * @returns true if data was successfully stored in the output variable.
	 *          false if no data was available, and done() has been called on
	 *                the queue
	 */
	bool get(Data & out);

	/**
	 * Add an element to the queue, blocking if the maximum queue size has
	 * been reached. Note that this method does nothing if done() has been
	 * called.
	 *
	 * @param element The data to add to the queue
	 *
	 * @throws std::runtime_error if shutdown() has been called
	 */
	void put(const Data & element);

	/**
	 * Signal that the producer(s) have finished adding data to the queue, and
	 * no more data will be added.
	 *
	 * @warning Calling this will cause put() to become a noop.
	 */
	void done();

	/**
	 * Cancel all actions on the queue, leaving it unusable. This may be used
	 * in case a producer or consumer encounters an unrecoverable error.
	 */
	void shutdown();

	unsigned callsToPut() const
	{
		return inserts_;
	}

	unsigned callsToGet() const
	{
		return extracts_;
	}

	/// queue status
	enum Status
	{
		Ready, //< Data may be inserted or extracted, possibly blocking
		Done,  //< The producer(s) have notified that they are done producing data
		Shutdown //< An error has occured, and this queue can not be used anymore
	};

	/**
	 * Get access to the queue's current status.
	 */
	Status status() const
	{
		return status_;
	}

private:

	std::deque<Data> que_;
	Status status_;

	boost::mutex mutex_;
	boost::condition condition_;

	unsigned maxQueSize_;
	std::string queName_;

	unsigned inserts_;
	unsigned extracts_;
};

} /* namespace fastload */
#endif /* DATAQUE_H_ */
