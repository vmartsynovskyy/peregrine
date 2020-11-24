#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Peregrine.hh"

int main(int argc, char *argv[])
{
  if (argc < 3)
  {
    std::cerr << "USAGE: " << argv[0] << " <# threads> <master hostname>" << std::endl;
    return -1;
  }

  size_t nthreads = std::stoi(argv[1]);
  std::string master_host = std::string(argv[2]);

  Peregrine::start_worker(nthreads, master_host);

  return 0;
}

