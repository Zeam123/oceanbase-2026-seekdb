#pragma once

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace observer
{

class AIColumnWorker : public common::ObTimerTask
{
public:
  void runTimerTask() override;
};

} // namespace observer
} // namespace oceanbase
