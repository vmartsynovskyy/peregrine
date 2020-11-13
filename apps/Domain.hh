#ifndef DOMAIN_HH
#define DOMAIN_HH

#include "roaring/roaring.hh"
#include "Peregrine.hh"
#include <cereal/archives/binary.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/types/string.hpp>
#include <memory>
#include <cstring>

// size_t
// get_serialized_size(const std::vector<Roaring> &sets)
// {
//   size_t buff_size = 0;
//   for (const auto &set : sets) {
//     size_t set_size = set.getSizeInBytes();
//     buff_size += set_size;
//   }
//   size_t header_size = sizeof(uint32_t) * sets.size();
//   buff_size += header_size;
//   return buff_size;
// }

/**
 * Serializes a vector of sets into an array of uint32_t offsets representing
 * the size of each set followed by the char array representation of each
 * set as given by roaring's write() function
 */
// void
// serialize_sets(const std::vector<Roaring> &sets, char* buf)
// {
//   std::vector<uint32_t> set_sizes;
//   size_t buff_size = 0;
//   for (const auto &set : sets) {
//     size_t set_size = set.getSizeInBytes();
//     buff_size += set_size;
//     set_sizes.emplace_back(set_size);
//   }
//   size_t header_size = sizeof(uint32_t) * sets.size();
//   buff_size += header_size;
//   // copy header and move pointer forward
//   std::memcpy(buf, &set_sizes[0], header_size);
//   buf += header_size;
// 
//   for (size_t i = 0; i < sets.size(); i++) {
//     // serialize set and move pointer forward
//     sets[i].write(buf);
//     buf += set_sizes[i];
//   }
// }

/**
 * Deserializes a vector of nsets roaring sets stored in the format describe
 * in serialize_sets
 */
std::vector<Roaring>
deserialize_sets(char* buf, size_t nsets)
{
  std::vector<Roaring> sets;

  // read header and move pointer forward
  size_t header_size = nsets * sizeof(uint32_t);
  std::vector<uint32_t> set_sizes(buf, buf + header_size); 
  buf += header_size;

  for (size_t i = 0; i < nsets; i++) {
    // read set and move pointer foward
    sets.push_back(Roaring::readSafe(buf, set_sizes[i]));
    buf += set_sizes[i];
  }

  return sets;
}

struct Domain
{
  /**
   * Default constructor is needed by Peregrine.
   */
  Domain() : sets(Peregrine::Context::current_pattern->num_aut_sets())
  {}

  Domain(size_t num_sets, bool always_true) : sets(num_sets)
  {}

  Domain(char* buf, size_t nsets)
  {
    sets = deserialize_sets(buf, nsets);
  }

  /**
   * 'Copy' constructor is needed by Peregrine.
   *
   * Note that this is not a true copy constructor since it copies not from
   * Domain but from what actually gets mapped in the callback in fsm.cc.
   * You could also use Domain instead of std::vector as the type for m, but
   * it might be less efficient.
   */
  Domain(const std::vector<uint32_t> &m) : sets(Peregrine::Context::current_pattern->num_aut_sets())
  {
    for (uint32_t i = 0; i < m.size(); ++i)
    {
      uint32_t u = m[i];
      uint32_t set = Peregrine::Context::current_pattern->aut_map[i];
      sets[set].add(u);
    }
  }

  /**
   * A sum operator is needed by Peregrine.
   */
  Domain &operator+=(const Domain &d)
  {
    uint32_t i = 0;
    for (const auto &set : d.get_sets())
    {
      sets[i++] |= set;
    }

    return *this;
  }

  /**
   * Specialization of the sum operator for efficiency.
   *
   * Avoids building Roaring bitsets with a single element just so they can
   * be added to another Domain. Add new elements directly.
   */
  Domain &operator+=(const std::vector<uint32_t> &m)
  {
    for (uint32_t i = 0; i < m.size(); ++i)
    {
      uint32_t u = m[i];
      uint32_t set = Peregrine::Context::current_pattern->aut_map[i];
      sets[set].add(u);
    }

    return *this;
  }

  /**
   * reset() method is required by Peregrine.
   *
   * Resets the data structure for the next pattern to be matched.
   */
  void reset()
  {
    sets.clear();
    sets.resize(Peregrine::Context::current_pattern->num_aut_sets());
  }

  /**
   * Returns trivially-copyable type for the view function.
   */
  uint64_t get_support()
  {
    uint64_t s = sets.front().cardinality();
    for (uint32_t i = 1; i < sets.size(); ++i)
    {
      s = std::min(s, sets[i].cardinality());
    }

    return s;
  }

  const std::vector<Roaring> &get_sets() const
  {
    return sets;
  }

  template<class Archive>
  void serialize(Archive &archive)
  {
    archive(sets);
  }

  uint32_t getN()
  {
    return sets.size();
  }

  std::vector<Roaring> sets;
};

struct DiscoveryDomain
{
  DiscoveryDomain() : sets(2)
  {}

  DiscoveryDomain(size_t num_sets, bool always_true) : sets(2)
  {}

  DiscoveryDomain(char* buf, size_t nsets)
  {
    sets = deserialize_sets(buf, nsets);
  }

  DiscoveryDomain(const std::pair<std::vector<uint32_t>, uint32_t> &p) : sets(1 + p.second)
  {
    auto &m = p.first;
    sets[0].add(m[0]);
    sets[1].add(m[1]);
    sets[p.second].add(m[2]); // if merge
  }

  DiscoveryDomain &operator+=(const DiscoveryDomain &d)
  {
    sets.resize(d.sets.size());

    uint32_t i = 0;
    for (const auto &set : d.get_sets())
    {
      sets[i++] |= set;
    }

    return *this;
  }

  DiscoveryDomain &operator+=(const std::pair<std::vector<uint32_t>, uint32_t> &p)
  {
    sets.resize(1+p.second);
    auto &m = p.first;
    sets[0].add(m[0]);
    sets[1].add(m[1]);
    sets[p.second].add(m[2]); // if merge

    return *this;
  }

  uint64_t get_support()
  {
    uint64_t s = sets.front().cardinality();
    for (uint32_t i = 1; i < sets.size(); ++i)
    {
      s = std::min(s, sets[i].cardinality());
    }

    return s;
  }

  void reset()
  {
    sets.clear();
    sets.resize(Peregrine::Context::current_pattern->num_aut_sets());
  }

  const std::vector<Roaring> &get_sets() const
  {
    return sets;
  }

  template<class Archive>
  void serialize(Archive &archive)
  {
    archive(sets);
  }

  uint32_t getN()
  {
    return sets.size();
  }

  std::vector<Roaring> sets;
};

template<class Archive>
void save(Archive & archive, Roaring const & r)
{ 
  auto r_size = r.getSizeInBytes(false);
  std::string data(r_size, '\0');
  r.write(data.data(), false);

  archive(data); 
}

template<class Archive>
void load(Archive & archive, Roaring & r)
{ 
  std::string data;
  archive(data); 

  r = r.read(data.data(), false);
}

#endif
