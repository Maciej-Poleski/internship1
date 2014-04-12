#include <iostream>
#include <random>
#include <functional>
#include <string>
#include <ctime>

// Generates big table with small id pool.

int main(int argc,char**argv)
{
    std::ios_base::sync_with_stdio(false);
    if(argc!=2)
    {
        std::cout<<argv[0]<<" [number of rows]\n";
        return 1;
    }
    std::mt19937 engine(time(nullptr));
    std::uniform_int_distribution<char> charDistribution('a','z');
    auto generateChar=std::bind(charDistribution,engine);
    std::cout<<"id,col1,col2\n";
    long rows=std::stol(argv[1]);
    for(long i=0;i<rows;++i)        // rows
    {
        for(decltype(i) j=0;j<3;++j)    // id
        {
            std::cout<<generateChar();
        }
        std::cout<<',';
        for(decltype(i) j=0;j<10;++j)    // col1
        {
            std::cout<<generateChar();
        }
        std::cout<<',';
        for(decltype(i) j=0;j<10;++j)    // col2
        {
            std::cout<<generateChar();
        }
        std::cout<<'\n';
    }
    return 0;
}