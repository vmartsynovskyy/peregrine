#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "Peregrine.hh"

bool is_directory(const std::string &path)
{
   struct stat statbuf;
   if (stat(path.c_str(), &statbuf) != 0)
       return 0;
   return S_ISDIR(statbuf.st_mode);
}

int main(int argc, char *argv[])
{
  if (argc < 3)
  {
    std::cerr << "USAGE: " << argv[0] << " <data graph> <# threads> <master hostname>" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  size_t nthreads = std::stoi(argv[2]);
  std::string master_host = std::string(argv[3]);

  if (is_directory(data_graph_name))
  {
    Peregrine::start_worker(data_graph_name, nthreads, master_host);
  }
  else
  {
    Peregrine::SmallGraph G(data_graph_name);
    Peregrine::start_worker(G, nthreads, master_host);
  }

  return 0;
}

