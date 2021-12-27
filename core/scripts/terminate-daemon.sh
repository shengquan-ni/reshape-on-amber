red=`tput setaf 1`
green=`tput setaf 2`
reset=`tput sgr0`

echo "${red}Terminating TexeraWebApplication at $(pgrep -f TexeraWebApplication)...${reset}"
kill -9 $(pgrep -f TexeraWebApplication)
echo "${green}Terminated.${reset}"
echo

echo "${red}Terminating TexeraRunWorker at $(pgrep -f TexeraRunWorker)...${reset}"
kill -9 $(pgrep -f TexeraRunWorker)
echo "${green}Terminated.${reset}"
