#pragma once

/* Copyright 2013 ClusterTech Ltd */

/**
 * @file
 *
 * Define Fim object classes and associated facilities.
 */
#include <stdint.h>

#include <iosfwd>

#include <boost/intrusive_ptr.hpp>  // IWYU pragma: export
// IWYU pragma: no_include "boost/smart_ptr/intrusive_ptr.hpp"

#include "common.hpp"
#include "intrusive_util.hpp"

namespace cpfs {

/**
 * The Fim object interface.  The purpose of the interface is to allow
 * code handling all Fim object types to be written.
 */
class IFim {
 public:
  virtual ~IFim() {}
  /** Field accessors */
  /**@{*/
  /**
   * Get the full FIM message.  Normally this is all the memory that
   * is owned by this object.
   */
  virtual const char* msg() const = 0;
  /**
   * Gets the length of the FIM message.
   */
  virtual uint32_t len() const = 0;
  /**
   * Gets the type of the FIM message.  Note that this cannot be
   * modified: different type of FIM is represented by different type
   * of Fim objects.
   */
  virtual FimType type() const = 0;
  /**
   * Get the final flag of the FIM message.  A reply should be marked
   * final if the request triggering it (1) is sent by an FC, and (2a)
   * doesn't need to be replicated, or (2b) has no standby MS
   * available for replication.  It should also be marked as final if
   * it is a FinalReplyFim.
   *
   * @return The final flag
   */
  virtual bool is_final() const = 0;
  /**
   * Set the final flag of the FIM message.
   *
   * @param final The new final flag
   */
  virtual void set_final(bool final = true) = 0;
  /**
   * Gets the request ID of the FIM message.
   */
  virtual ReqId req_id() const = 0;
  /**
   * Sets the request ID of the FIM message.
   *
   * @param req_id The new request ID.
   */
  virtual void set_req_id(ReqId req_id) = 0;
  /**
   * The protocol of the FIM message.  The byte-order of the FIM is
   * also detected using this.
   */
  virtual uint16_t protocol() const = 0;
  /**
   * Whether this is a reply Fim
   */
  virtual bool IsReply() const = 0;
  /**@}*/
  /** Body and tail buffer */
  /**@{*/
  /**
   * Get the body.  It should be part of the message, but is writable.
   */
  virtual char* body() = 0;
  /**
   * Get the size of the body.
   */
  virtual uint32_t body_size() const = 0;
  /**
   * Gets the tail buffer.  It should be part of the message, but is
   * writable.
   */
  virtual char* tail_buf() = 0;
  /**
   * Gets the tail buffer, for const IFim objects.
   */
  virtual const char* tail_buf() const = 0;
  /**
   * Gets the size of the tail buffer.
   */
  virtual uint32_t tail_buf_size() const = 0;
  /**
   * Resize the tail buffer.  Calling this function might copy the
   * message to new location, and thus invalidates all pointers
   * returned previously by all method of the same Fim object.
   *
   * @param new_size The new size.
   */
  virtual void tail_buf_resize(uint32_t new_size) = 0;
  /**@}*/
 protected:
  friend void intrusive_ptr_add_ref(const IFim* obj);
  friend void intrusive_ptr_release(const IFim* obj);
  /**
   * @return The reference count object for boost::intrusive_ptr
   */
  virtual SimpleRefCount<IFim>* cnt() const = 0;
};

/**
 * Add a reference to an IFim.  For use with boost::intrusive_ptr.
 *
 * @param obj The IFim
 */
inline void intrusive_ptr_add_ref(const IFim* obj) {
  obj->cnt()->AddRef();
}

/**
 * Remove a reference to an IFim.  For use with boost::intrusive_ptr.
 *
 * @param obj The IFim
 */
inline void intrusive_ptr_release(const IFim* obj) {
  obj->cnt()->Release(obj);
}

/** Smart pointer type for Fim objects */
#define FIM_PTR boost::intrusive_ptr

/**
 * Head of Fim.  All FIM starts with this.  Note that the design of
 * the FIM head takes care of alignment.
 */
struct FimHead {
  uint32_t len; /**< The length of the FIM. */
  FimType type; /**< The type of the FIM, top bit stores final flag. */
  uint16_t protocol; /**< The protocol of the FIM. */
  ReqId req_id; /**< The ID of the FIM. */
};

/**
 * Print a Fim to a stream.
 *
 * @param ost The stream to print to
 *
 * @param socket The FimSocket to print
 */
std::ostream& operator<<(std::ostream& ost, const IFim& socket);

}  // namespace cpfs
