#pragma once

namespace streamit::cli {

// Run the admin command
int RunAdmin(int argc, char* argv[]);

// Admin subcommands
int RunCreateTopic(int argc, char* argv[]);
int RunDescribeTopic(int argc, char* argv[]);
int RunListTopics(int argc, char* argv[]);

// Help functions
void PrintAdminHelp();
void PrintCreateTopicHelp();
void PrintDescribeTopicHelp();
void PrintListTopicsHelp();

}

