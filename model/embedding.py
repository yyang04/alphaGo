import torch
from torch import nn
from torch import nn, Tensor
import torch.nn.functional as F


class NumEmbedding(nn.Module):
    """
    连续特征用linear层编码
    输入shape: [batch_size, features_num(n), d_in], # d_in 通常是1
    输出shape: [batch_size, features_num(n), d_out]
    """

    def __init__(self, n: int, d_in: int, d_out: int, bias: bool = False) -> None:
        super().__init__()
        self.weight = nn.Parameter(Tensor(n, d_in, d_out))
        self.bias = nn.Parameter(Tensor(n, d_out)) if bias else None
        with torch.no_grad():
            for i in range(n):
                layer = nn.Linear(d_in, d_out)
                self.weight[i] = layer.weight.T
                if self.bias is not None:
                    self.bias[i] = layer.bias

    def forward(self, x_num):
        # x_num: batch_size, features_num, d_in
        assert x_num.ndim == 3
        x = torch.einsum("bfi,fij->bfj", x_num, self.weight)
        if self.bias is not None:
            x = x + self.bias[None]
        return x


class CatEmbedding(nn.Module):
    """
    输入shape: [bs, num_feat],
    输出shape: [bs, num_feat, dim]
    """

    def __init__(self, categories, dim):
        super().__init__()
        self.embedding = nn.Embedding(sum(categories), dim)
        self.offsets = nn.Parameter(torch.tensor([0] + categories[:-1]).cumsum(0), requires_grad=False)
        torch.nn.init.xavier_uniform_(self.embedding.weight.data)

    def forward(self, x_cat):
        x = x_cat + self.offsets[None]
        return self.embedding(x)


class CatLinear(nn.Module):
    """
    离散特征用Embedding实现线性层（等价于先F.onehot再nn.Linear()）
    输入shape: [bs, features_num],
    输出shape: [bs, d_out]
    """

    def __init__(self, categories, d_out=1):
        super().__init__()
        self.fc = nn.Embedding(sum(categories), d_out)
        self.bias = nn.Parameter(torch.zeros((d_out,)))
        self.offsets = nn.Parameter(torch.tensor([0] + categories[:-1]).cumsum(0), requires_grad=False)

    def forward(self, x_cat):
        """
        :param x: Long tensor of size ``(batch_size, features_num)``
        """
        x = x_cat + self.offsets[None]
        return torch.sum(self.fc(x), dim=1) + self.bias


class FMLayer(nn.Module):
    """
    FM交互项
    """

    def __init__(self, reduce_sum=True):
        super().__init__()
        self.reduce_sum = reduce_sum

    def forward(self, x):  #注意：这里的x是公式中的 <v_i> * xi
        """
        :param x: Float tensor of size ``(batch_size, num_features, k)``
        """
        square_of_sum = torch.sum(x, dim=1) ** 2
        sum_of_square = torch.sum(x ** 2, dim=1)
        ix = square_of_sum - sum_of_square
        if self.reduce_sum:
            ix = torch.sum(ix, dim=1, keepdim=True)
        return 0.5 * ix


class FM(nn.Module):
    """
    完整FM模型。
    """

    def __init__(self, d_numerical, categories=None, d_embed=4,
                 n_classes=1):
        super().__init__()
        if d_numerical is None:
            d_numerical = 0
        if categories is None:
            categories = []
        self.categories = categories
        self.n_classes = n_classes

        self.num_linear = nn.Linear(d_numerical, n_classes) if d_numerical else None
        self.cat_linear = CatLinear(categories, n_classes) if categories else None

        self.num_embedding = NumEmbedding(d_numerical, 1, d_embed) if d_numerical else None
        self.cat_embedding = CatEmbedding(categories, d_embed) if categories else None

        if n_classes == 1:
            self.fm = FMLayer(reduce_sum=True)
            self.fm_linear = None
        else:
            assert n_classes >= 2
            self.fm = FMLayer(reduce_sum=False)
            self.fm_linear = nn.Linear(d_embed, n_classes)

    def forward(self, x):

        """
        x_num: numerical feature
        x_cat: category feature
        """
        x_num, x_cat = x

        # linear部分
        x = 0.0
        if self.num_linear:
            x = x + self.num_linear(x_num)
        if self.cat_linear:
            x = x + self.cat_linear(x_cat)

        # 交叉项部分
        x_embedding = []
        if self.num_embedding:
            x_embedding.append(self.num_embedding(x_num[..., None]))
        if self.cat_embedding:
            x_embedding.append(self.cat_embedding(x_cat))
        x_embedding = torch.cat(x_embedding, dim=1)

        if self.n_classes == 1:
            x = x + self.fm(x_embedding)
            x = x.squeeze(-1)
        else:
            x = x + self.fm_linear(self.fm(x_embedding))
        return x
